set xlabel  "clients" 
set ylabel "throughput(op/s)"
set auto x 
set auto y 
#set xrange [0:500]
#set yrange [0:75]
#set xtics offset 1
#set xtics offset 1
set terminal postscript eps blacktext monochrome font 'Times-Roman,24'
set output 'throughput_client.eps'
#set key font "Times-Roman,36" 
#set xtics font "Times-Roman,36" 
#set ytics font "Times-Roman,36" 
#set title  font "Times-Roman,36" 'Online Retail Application'
plot 'throughput_client.csv' using 1:2 title 'with MYSQL Cluster'  with linespoints pointtype 5 pointsize 1, 'throughput_client.csv' using 3:4 title 'With WeaQL'  with linespoints pointtype 2 pointsize 1;