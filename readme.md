## Initial setup (Centos 6.8)

```
$ git clone https://github.com/nk53/dht.git ~/dht
$ cd ~/dht
$ mv bin ~
$ mv setup_env.sh ~
$ cd
$ ./setup_env.sh
```

The DHT project uses Python version 3.6, which is not the default version
of Python on Centos 6.8. Instead, a virtual environment is created where
Python 3.6 is the default version of Python.

Once you've run the above commands, you can activate the virtual
environment by using the following command (alias) from the home directory:
```
    $ dht
```
This will also automatically place you in the DHT main project directory
located at ~/dht

If you don't want to activate a virtual environment, you can also access
Python 3.6 directly using any of these commands:
```
    $ python3.6
    $ python3    # symlink to above
    $ py3        # another symlink to above
```
For more information on setting up a virtual environment for Python 3 on a
fresh installation of Centos 6/7, see [here](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-centos-7).
