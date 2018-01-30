set xlabel  "clients" 
set ylabel "latency(ms)"
#set auto x 
set auto y 
#set xrange [0:500]
set yrange [0:15]
#set xtics offset 1
#set xtics offset 1
set terminal postscript eps blacktext monochrome font 'Times-Roman,24'
set output 'latency_client.eps'
#set key font "Times-Roman,36" 
#set xtics font "Times-Roman,36" 
#set ytics font "Times-Roman,36" 
#set title  font "Times-Roman,36" 'Online Retail Application'
plot 'latency_client.csv' using 1:2 title 'with MYSQL Cluster'  with linespoints pointtype 5 pointsize 1, 'latency_client.csv' using 3:4 title 'With WeaQL'  with linespoints pointtype 2 pointsize 1;