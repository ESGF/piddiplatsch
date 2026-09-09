# Deployment

Check out Piddi manually, normally below `/opt/piddiplatsch`, then create its
project-local Conda environment and install Piddi into it:

```console
cd /opt/piddiplatsch
conda env create --prefix .conda --file environment.yml
conda run --prefix .conda python -m pip install -e .
```

Configure its executable and service:

```console
python -m pip install ansible-core
cp deploy/ansible/custom.yml.example deploy/ansible/custom.yml
vim deploy/ansible/custom.yml
make play
```

Ansible creates the service user and directories, renders
`/etc/piddi/piddi.toml`, and validates it, but does not start Piddi. Test it:

```console
sudo runuser -u piddi -- /opt/piddiplatsch/.conda/bin/piddi \
  --config /etc/piddi/piddi.toml --silent consume
```

Then set `piddi_enable_service: true` in `custom.yml` and run `make play` again.

## Vagrant test VM

On an Apple Silicon Mac with Parallels:

```console
vagrant plugin install vagrant-parallels
vagrant up
vagrant ssh
```

Inside the AlmaLinux VM:

```console
cd /vagrant
cp deploy/ansible/custom.yml.example deploy/ansible/custom.yml
vim deploy/ansible/custom.yml
make play ANSIBLE_ARGS=
```

Remove the VM later with `vagrant destroy` on the Mac.
