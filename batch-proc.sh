./ftp_urls_echo.sh | parallel && rm *.xml.tar.gz && gsutil -m cp -r ./ gs://experimental-core-store
