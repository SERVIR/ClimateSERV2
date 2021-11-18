file_date=$(date +'%m.%d.%Y')

mv /cserv2/tmp/access.log /cserv2/tmp/access.${file_date}.log
kill -USR1 `cat master.nginx.pid`
sleep 1
gzip /cserv2/tmp/access.${file_date}.log