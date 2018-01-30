# Set terminal & output
set terminal postscript eps enhanced color solid font 'Helvetica,25'

# Output
set output outputfile

# Axis
#set yrange [0:400]
set ylabel "Latency (ms)"
set xrange [0:*]
set xlabel "Throughput (ops/s)"
set xtics nomirror rotate by -30 scale 0.5
set xtics font "Helvetica,16"

# Key
set key vertical
set key top right

# Grid
set style line 12 lc rgb '#808080' lt 0 lw 1
set grid back ls 12

# Plot type
set style data linespoints

#RGB Colors
red = '#a2142f'
yellow='#edb120'
green='#669933' # green
purple='#7e2f8e' # purple
orange='#d95319' # orange
blue = '#0056bd'

# Lines
#set style line 1 linecolor rgb '#0060ad' linetype 1 linewidth 2 pointtype 1 pointsize 1.5   # --- blue
#set style line 1 lc rgb '#0060ad' lt 1 lw 1 pt 1 ps 2
#set style line 2 lc rgb '#0060ad' lt 1 lw 1 pt 2 ps 2
#set style line 3 lc rgb '#dd181f' lt 1 lw 2 pt 3 ps 2
#set style line 4 lc rgb '#dd181f' lt 1 lw 1 pt 4 ps 2
#set style line 5 lc rgb '#0060ad' lt 1 lw 1 pt 5 ps 2
#set style line 6 lc rgb '#82CA4A' lt 1 lw 1 pt 6 ps 2
#set style line 7 lc rgb '#dd181f' lt 1 lw 1 pt 7 ps 2

#every A:B:C:D:E:F
#A: line increment
#B: data block increment
#C: The first line
#D: The first data block
#E: The last line
#F: The last data block

set datafile separator ','

plot data1 every 4::1 using ($3/60):7 with linespoints ls 5 lc rgb '#006400' lw 2.0 ps 1.5 title 'WeaQL',\
data2 every 4::1 using ($3/60):7 with linespoints ls 7 lc rgb purple lw 2.0 ps 1.6 title 'Galera-Cluster',\
data3 every 4::1 using ($3/60):7 with linespoints ls 9 lc rgb orange lw 2.0 ps 1.9 title 'MySQL-Cluster'




