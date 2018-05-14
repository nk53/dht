#include <pthread.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/errno.h>

// also includes stdio.h, stdlib.h, and string.h
#include <Python.h>

/* request/response formatting macros */
#define RESPONSE_LEN    5
#define REQUEST_LEN     7
/* maximum # of un-handled messages */
#define BACKLOG_SIZE    1000

#define NUM_THREADS     10000
#define NUM_KEYS        10000
/* leave some room for meta-data */
#define TABLE_SIZE      sizeof(int) * (NUM_KEYS + 1)
#define SHARED_MEM_SIZE sizeof(shared_mem_t)
/* table[NUM_KEYS] stores the number of threads that have received END */
#define IS_DONE         table[NUM_KEYS] == NUM_THREADS

/* request/response double-ended queue types */
typedef struct request_queue_t {
    char queue[BACKLOG_SIZE][REQUEST_LEN];
    unsigned short size; // number of items in queue
    size_t left;  // left end of queue
    size_t right; // right end of queue
} request_queue_t;

typedef struct response_queue_t {
    char queue[BACKLOG_SIZE][RESPONSE_LEN];
    unsigned short size; // number of items in queue
    size_t left;  // left end of queue
    size_t right; // right end of queue
} response_queue_t;

/* indicates whether a key is pending, and which message ID is waiting */
typedef struct pending_map_t {
    char is_pending;
    unsigned short message_id;
} pending_map_t;

typedef struct shared_mem_t {
    int table[TABLE_SIZE];
    int pending_map_t[NUM_KEYS];
    request_queue_t request_queue;
    response_queue_t response_queue;
} shared_mem_t;

typedef struct thread_data_t {
    long thread_id;
    shared_mem_t *shared_mem;
} thread_data_t;

/* Locks and conditions */
pthread_mutex_t table_mutex[NUM_KEYS];
pthread_mutex_t request_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t response_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t request_queue_not_empty = PTHREAD_COND_INITIALIZER;
pthread_cond_t response_queue_not_empty = PTHREAD_COND_INITIALIZER;

pthread_cond_t request_queue_not_full = PTHREAD_COND_INITIALIZER;
pthread_cond_t response_queue_not_full = PTHREAD_COND_INITIALIZER;

/* Data about threads */
pthread_t threads[NUM_THREADS];
thread_data_t thread_data[NUM_THREADS];

/*
 * global variable referencing shared memory; only main thread methods
 * should use this
 */
shared_mem_t *_shared_mem;

void *sayHi(void *my_thread_data)
{
    // TODO: remove this
    pthread_exit(NULL);

    thread_data_t *my_data = (thread_data_t *) my_thread_data;
    long tid = my_data->thread_id;
    int *table = my_data->shared_mem->table;
    // avoid race condition w/ lock
    pthread_mutex_lock(&table_mutex[tid]);
    int my_value = table[tid];
    pthread_mutex_unlock(&table_mutex[tid]);
    printf("Thread %ld wrote: %d\n", tid, my_value);
    pthread_exit(NULL);
}

/**
 * Initializes POSIX threads and shared memory, then returns Py_None
 */
static PyObject *
thread_init(PyObject *self, PyObject *args)
{
    /* TODO: handle args */

    // setup mutexes
    for (int i = 0; i < NUM_KEYS; ++i)
    {
        pthread_mutex_init(&table_mutex[i], NULL);
    }
    
    // create shared memory
    void *region = mmap(0, SHARED_MEM_SIZE, // allocate new region
        PROT_READ | PROT_WRITE,             // allow reading and writing
        MAP_ANON | MAP_SHARED, 0, 0);       // we're not using a file

    if (region == MAP_FAILED)
    {
        perror("mmap error");
        exit(-2);
    }

    // ensure region is zero-filled
    memset(region, 0, SHARED_MEM_SIZE);

    // save shared memory to global variable
    _shared_mem = (shared_mem_t *) region;
    int *table = _shared_mem->table;

    // give initial values in the shared memory region
    for (int i = 0; i < NUM_KEYS; ++i)
    {
       table[i] = i;
    }

    long t;
    for (t = 0; t < NUM_THREADS; t++)
    {
        thread_data[t].thread_id = t;
        thread_data[t].shared_mem = _shared_mem;
    }

    int result;
    for (t = 0; t < NUM_THREADS; t++)
    {
        //printf("Creating thread %ld\n", t);
        result = pthread_create(&threads[t], NULL, sayHi,
                (void *) &thread_data[t]);

        if (result)
        {
            printf("pthread_create Error (%d)\n", result);
            exit(-1);
        }
    }

    Py_RETURN_NONE;
}

/*
 * Waits for all threads to exit, un-maps shared memory, then returns
 * Py_None
 */
static PyObject *
thread_join(PyObject *self, PyObject *args)
{
    void *status;
    long t;
    int result;
    for (t = 0; t < NUM_THREADS; t++)
    {
        //printf("Joining thread %ld\n", t);
        result = pthread_join(threads[t], &status);
        if (result)
        {
            printf("Something has gone horribly wrong... (%d)\n", result);
            exit(-1);
        }
    }

    printf("Done; un-mapping memory\n");
    munmap(_shared_mem, SHARED_MEM_SIZE);

    // clean up mutexes
    for (int i = 0; i < NUM_KEYS; ++i)
    {
        pthread_mutex_destroy(&table_mutex[i]);
    }

    pthread_exit(NULL);
}

/* TODO: Finish this */
static PyObject *
thread_put_request(PyObject *self, PyObject *args)
{
    // Python args to this function
    PyBytesObject *bytes_obj;
    int unused;  // at the moment ignored, might be used later

    // what we want out of the bytes object
    const char *byte_str;

    // get exactly a Bytes object and an int, else fail and return
    if (!PyArg_ParseTuple(args, "Si", &bytes_obj, &unused))
        return NULL;

    byte_str = PyBytes_AsString((PyObject *) bytes_obj);

    // check whether there is enough space in the queue
    request_queue_t *request_queue = &_shared_mem->request_queue;
    pthread_mutex_lock(&request_mutex);
    if (request_queue->size >= BACKLOG_SIZE)
    {
        pthread_mutex_unlock(&request_mutex);
        Py_RETURN_FALSE;
    }

    // update the queue's size
    ++request_queue->size;

    // determine how the right queue pointer should be updated
    size_t right = request_queue->right;
    if (right > BACKLOG_SIZE)
    {
        // reset right end to left side
        request_queue->right = right = 0;
    }
    else
    {
        // just move it more to the right
        ++right;
    }

    // copy the message to the next slot in the request_queue
    memcpy(&request_queue->queue[right], byte_str, REQUEST_LEN);

    pthread_mutex_unlock(&request_mutex);
    Py_RETURN_TRUE;
}

/* TODO: Finish this */
static PyObject *
thread_get_response(PyObject *self, PyObject *args)
{
    // Python args to this function
    PyBytesObject *bytes_obj;
    int unused;  // at the moment ignored, might be used later

    // what we want out of the bytes object
    const char *byte_str;
    unsigned short message_id;

    // get exactly a Bytes object and an int, else fail and return
    if (!PyArg_ParseTuple(args, "Si", &bytes_obj, &unused))
        return NULL;

    byte_str = PyBytes_AsString((PyObject *)bytes_obj);
    //unsigned short message_id = (unsigned short) *byte_str;

    // get ushort from bytes object
    memcpy(&message_id, byte_str, 2);

    Py_RETURN_NONE;
}

/*
 * TODO: replace get_request with get_response; move get_request code to
 * pthread instance code
 */
static PyMethodDef ThreadMethods[] = {
    {"init", thread_init, METH_VARARGS, "Setup POSIX threads and mmap"},
    {"join", thread_join, METH_VARARGS, "Clean up threads and mmap"},
    {"put_request",thread_put_request, METH_VARARGS,
        "Appends a new request bytes-like object to the request queue"},
    {"get_request", thread_get_response, METH_VARARGS,
        "Pops the first request bytes-like object from the request queue"}
};

static struct PyModuleDef threadmodule = {
    PyModuleDef_HEAD_INIT,
    "thread",
    "For managing POSIX threads",
    -1,
    ThreadMethods
};

PyMODINIT_FUNC
PyInit_thread(void)
{
    return PyModule_Create(&threadmodule);
}
