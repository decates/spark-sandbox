# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "bento/ubuntu-16.04"
  config.vm.box_check_update = false
  config.vm.network "private_network", ip: "10.0.1.2"
  config.vm.synced_folder "./data", "/home/vagrant/data"
  config.vm.synced_folder "./scripts", "/home/vagrant/scripts"

  # Don't use the key, as we need to provide a different one for Spark access
  config.ssh.insert_key = false
  config.ssh.username = "vagrant"
  config.ssh.password = "vagrant"
  
  config.vm.provider "virtualbox" do |vb|
    vb.gui = false
    vb.memory = "6500"
	vb.cpus = 5
  end
  
  config.vm.provision "shell", privileged: false, path: "setup/install-prerequisites.sh"
end
