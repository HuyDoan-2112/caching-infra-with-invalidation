FROM mysql:8.4
# Bake my.cnf in with 0644. Bind-mounting it from Windows makes it world-writable (0777),
# and MySQL ignores world-writable option files.
COPY --chmod=0644 my.cnf /etc/mysql/conf.d/my.cnf
