# Ploudeville Ansible Deployment

This directory deploys the Ploudeville web app to a dedicated Debian/Ubuntu server.

## What it configures
- System packages: `git`, `python3`, `python3-venv`, `nginx`
- App runtime user/group and app directories under `/opt/ploudeville`
- Git checkout of this repository
- Python virtual environment
- Systemd service for `python -m src.family_system.web`
- Nginx reverse proxy on port 80

## 1) Update inventory + variables

Edit:
- `inventories/production/hosts.yml`
- `group_vars/all.yml`

Set at minimum:
- `ansible_host`, `ansible_user`
- `app_repo_url` (SSH URL the server can access)
- `public_domain`
- `family_web_secret`

## 2) Run deploy

From `provisioning/ansible`:

```bash
ansible-playbook playbooks/deploy_ploudeville.yml
```

## 3) Verify

```bash
ansible webservers -m ansible.builtin.systemd -a "name=ploudeville.service"
ansible webservers -m ansible.builtin.uri -a "url=http://127.0.0.1:18000/ return_content=no status_code=200"
```

## Notes
- For private repos, install a deploy key on the target host so Ansible can pull `app_repo_url`.
- Secrets currently live in `group_vars/all.yml`; move them to `ansible-vault` before production use.
- TLS is not configured here yet. Add Certbot or your existing TLS edge in a follow-up.
