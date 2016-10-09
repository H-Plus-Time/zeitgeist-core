# curl "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/articles.$1.xml.tar.gz" -o $1 && tar -xvf $1 -C articles
echo "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/articles.$1.xml.tar.gz"
