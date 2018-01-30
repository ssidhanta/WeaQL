set xlabel  "throughput(op/s)" 
set ylabel "latency(ms)"
set auto x 
set auto y 
#set xrange [0:500]
#set yrange [0:75]
#set xtics offset 1
#set xtics offset 1
set terminal postscript eps blacktext monochrome font 'Times-Roman,24'
set output 'throughput_latency.eps'
#set key font "Times-Roman,36" 
#set xtics font "Times-Roman,36" 
#set ytics font "Times-Roman,36" 
#set title  font "Times-Roman,36" 'Online Retail Application'
plot 'throughput_latency_replica.csv' using 2:1 title 'with MYSQL Cluster'  with linespoints pointtype 5 pointsize 1, 'throughput_latency_replica.csv' using 4:3 title 'With WeaQL'  with linespoints pointtype 2 pointsize 1;