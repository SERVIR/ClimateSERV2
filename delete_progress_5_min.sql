delete
from api_request_progress
where date_created < (NOW() - INTERVAL '5 MINUTE');