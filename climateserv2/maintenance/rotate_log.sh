file_date=$(date +'%m.%d.%Y')

mv /var/log/nginx/access.log /var/log/nginx/access.${file_date}.log
kill -USR1 `cat master.nginx.pid`
sleep 1
gzip /var/log/nginx/access.${file_date}.log