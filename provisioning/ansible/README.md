# Ploudeville Ansible Deployment

This directory deploys the Ploudeville web app to a dedicated Debian/Ubuntu server (including Raspberry Pi OS 64-bit).

## What it configures
- `bind9` on the Raspberry Pi for LAN DNS
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
- `app_repo_url` (SSH URL the server can access; no placeholder values)
- `public_domain` (`www.ploudeville.com` for LAN-only)
- `family_web_secret` (24+ chars; no placeholder value)
- Optional migration input: `app_db_seed_local_path` to a local `family.db` backup for first production deploy

Review and adjust for your LAN:
- `dns_forwarders` to the upstream resolvers you want bind9 to use for non-`ploudeville.com` lookups
- `dns_allow_query` / `dns_allow_recursion` if your LAN ACL is more specific than `localnets`
- `dns_server_ipv4` if the nginx host IP differs from `ansible_host`

## 2) Run deploy

From `provisioning/ansible`:

```bash
ansible-playbook playbooks/deploy_ploudeville.yml
```

## 3) Verify

```bash
ansible webservers -m ansible.builtin.systemd -a "name=ploudeville.service"
ansible webservers -m ansible.builtin.uri -a "url=http://127.0.0.1:18000/ return_content=no status_code=200"
ansible dnsservers -m ansible.builtin.command -a "dig @127.0.0.1 www.ploudeville.com +short"
ansible dnsservers -m ansible.builtin.command -a "dig @127.0.0.1 anything.ploudeville.com +short"
```

## Notes
- For private repos, install a deploy key on the target host so Ansible can pull `app_repo_url`.
- Secrets currently live in `group_vars/all.yml`; move them to `ansible-vault` before production use.
- TLS is not configured here yet. Add Certbot or your existing TLS edge in a follow-up.
- LAN clients must use the Raspberry Pi (`10.0.100.167`) as their DNS server, usually via your router DHCP settings, for the local override to apply.

## Raspberry Pi migration quick path (10.0.100.167)
1. Confirm SSH works:
```bash
ssh pi@10.0.100.167
```
2. If migrating data from dev host, copy your SQLite file to the Ansible controller and set:
```yaml
app_db_seed_local_path: "/absolute/path/to/family.db"
```
3. Set a real repo URL + secret in `group_vars/all.yml`.
4. Deploy:
```bash
cd provisioning/ansible
ansible-playbook playbooks/deploy_ploudeville.yml
```
