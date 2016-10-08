sudo service cassandra stop
sudo rm -r /cassandra/data/* /cassandra/commitlog/*
sudo service cassandra start
