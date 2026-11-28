FROM mysql:8.4
# Bake my.cnf in with 0644. Bind-mounting it from Windows makes it world-writable (0777),
# and MySQL ignores world-writable option files.
COPY --chmod=0644 my.cnf /etc/mysql/conf.d/my.cnf
# Bake the schema in too, so Compose and kind run the same image. The entrypoint runs
# these files once, when the data directory is empty.
COPY --chmod=0644 init/ /docker-entrypoint-initdb.d/
