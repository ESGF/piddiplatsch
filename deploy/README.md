# Deployment

The Ansible playbook installs one Piddi service on the local VM.

```console
python -m pip install ansible-core
cp deploy/ansible/custom.yml.example deploy/ansible/custom.yml
vim deploy/ansible/custom.yml
make play
```

The first run installs and validates Piddi but does not start it. Test it:

```console
sudo runuser -u piddi -- /opt/piddi/venv/bin/piddi \
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
