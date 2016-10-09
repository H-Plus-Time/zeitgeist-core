./ftp_urls_echo.sh | parallel && rm *.xml.tar.gz && gsutil -m cp -r ./articles gs://experimental-core-store
