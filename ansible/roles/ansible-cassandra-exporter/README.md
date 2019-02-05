Role Name
=========

Role to install `cassandra exporter java binary` as a `systemd` service

Role Variables
--------------

| Name           | Default Value | Description                        |
| -------------- | ------------- | -----------------------------------|
| `cassandra_exporter_version` | 2.2.0 | Cassandra exporter package version |
| `cassandra_listen_address` | 0.0.0.0 | Cassandra exporter listening address |
| `cassandra_listen_port` | 8080 | Cassandra exporter listening port |
| `cassandra_exporter_binary_url` | https://github.com/criteo/cassandra_exporter/releases/download/{{cassandra_exporter_version}}/cassandra_exporter-{{cassandra_exporter_version}}-all.jar | Cassandra exporter jar download location |
| `cassandra_exporter_config_url` | https://raw.githubusercontent.com/criteo/cassandra_exporter/master/config.yml | Cassandra exporter config download location |
| `cassandra_exporter_user` | cassandra-exp | UNIX user to run the binary |
| `cassandra_exporter_group` | cassandra-exp | UNIX group to run the binary |
| `cassandra_exporter_root_dir` | /opt/cassandra_exporter | Base location where cassandra exporter stuff is downloaded |
| `cassandra_exporter_dist_dir` | /opt/cassandra_exporter/dist | Location for binary and systemd service script |
| `cassandra_exporter_config_dir` | /opt/cassandra_exporter/config | Location for config |


Example Playbook
----------------

Including an example of how to use your role (for instance, with variables passed in as parameters) is always nice for users too:

    - hosts: servers
      become: true
      become_method: sudo
      roles:
        - role: ansible-cassandra-exporter

Usage
-----

You may install the role locally using `ansible-galaxy install -r requirements.yml -p roles/` where `requirements.yml` reads something like

```
- src: git+https://github.com/criteo/cassandra_exporter/ansible/roles
  version: TBD
```
