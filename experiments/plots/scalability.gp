# Set terminal & output
set terminal postscript eps enhanced color solid font 'Helvetica,25'

# Output
set output outputfile

# Axis
set yrange [0:2500]
set ylabel "Throughput (ops/s)"
set xrange [0:5.5]
set xlabel "Servers"
#set xtics nomirror rotate by -30 scale 0.5
#set xtics font "Helvetica,16"

# Key
set key top left

# Grid
set style line 12 lc rgb '#808080' lt 0 lw 0.5
set grid back ls 12

#RGB Colors
red = '#a2142f'
yellow='#edb120'
green='#77ac30' # green
purple='#7e2f8e' # purple
orange='#d95319' # orange
blue = '#0056bd'

# Plot type
set style data linespoints

# Lines
#set style line 1 linecolor rgb '#0060ad' linetype 1 linewidth 2 pointtype 1 pointsize 1.5   # --- blue
set style line 1 lc rgb '#00ad1a' lt 1 lw 2 pt 9 ps 2 # green
set style line 2 lc rgb '#0060ad' lt 1 lw 2 pt 2 ps 2
set style line 3 lc rgb '#dd181f' lt 1 lw 2 pt 3 ps 2
set style line 4 lc rgb '#dd181f' lt 1 lw 2 pt 4 ps 2
set style line 5 lc rgb '#0060ad' lt 1 lw 2 pt 5 ps 2
set style line 6 lc rgb '#82CA4A' lt 1 lw 2 pt 6 ps 2

set datafile separator ','


#every A:B:C:D:E:F
#A: line increment
#B: data block increment
#C: The first line
#D: The first data block
#E: The last line
#F: The last data block

#A:B:C:D:E:F -> 1 replica

plot \
data1 every 1:::0::0 using 1:($3/60) with linespoints ls 5 lc rgb '#006400' lw 2.0 ps 1.5 title 'WeaQL', \
data1 every 1:::1::1 using 1:($3/60) with linespoints ls 7 lc rgb purple lw 2.0 ps 1.6 title 'Galera-Cluster', \
data1 every 1:::2::2 using 1:($3/60) with linespoints ls 9 lc rgb orange lw 2.0 ps 1.9 title 'MySQL-Cluster'




