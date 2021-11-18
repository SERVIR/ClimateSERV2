file_date=$(date +'%m.%d.%Y')

mv /var/log/nginx/access.log /var/log/nginx/access.${file_date}.log
mv /var/log/nginx/error.log /var/log/nginx/error.${file_date}.log
kill -USR1 `cat /run/nginx.pid`
sleep 1
gzip /var/log/nginx/access.${file_date}.log
gzip /var/log/nginx/error.${file_date}.log