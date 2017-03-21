Following are the instruction for part 1 and part 2.

Part 1.
	(assume number of process =4, outputfile=“file.txt”)
	compile the code with the command:
	“mpicc -w mpi_io_p1.c -o mpi_io_p1”

	run with the command(1 parameter [output_filename]): 
	“mpiexec -n 4 ./mpi_io_p1 file.txt”

	use od to check the correctness of output file:
	“od -td -v file.txt”

Part 2.
	1. follow the orangeFS Documentation to build and configure, add server and add client in jarvis.
	2. build a new version of mpich, mpich-3.2 on jarvis. follow the command:
	“
	sed -i s/ADIOI_PVFS2_IReadContig/NULL/  src/mpi/romio/adio/ad_pvfs2/ad_pvfs2.c
	sed -i s/ADIOI_PVFS2_IWriteContig/NULL/ src/mpi/romio/adio/ad_pvfs2/ad_pvfs2.c

	/home/jzhu57/soft/mpich-3.2/configure --prefix=/home/jzhu57/soft/mpich-install --enable-romio --enable-shared --with-pvfs2=/home/jzhu57/soft/orangefs --with-file-system=pvfs2 --disable-fortran --disable-cxx

	make 
	make install
	”
	3. In the source code directory,compile the “mpi_io_p2.c” code with command(based on my jarvis account):
	“/home/jzhu57/soft/mpich-install/bin/mpicc -g mpi_io_p2.c -o mpi_io_p2 -L /home/jzhu57/soft/orangefs/lib -L /home/jzhu57/soft/mpich-install/lib -I /home/jzhu57/soft/orangefs/include -I /home/jzhu57/soft/mpich-install/include  -lpvfs2 -lmpich”
	4. use the bash file to run, for example if you wanna run on 8 process with each rank has 16MB data size. In the bash file, find the bash file named “run32.sh”, run with command:
	“qsub -pe mpich 8 run16.sh” when we want to control the number of node and run on specific nodes, run with the command(for example on 0-0,0-1,3-3,2-1):
	“qsub -l h=“gpu-compute-0-0.local|gpu-compute-0-1.local|gpu-compute-3-3.local|gpu-compute-2-1.local” -pe mpich 8 run16.sh”


ps:
1.Parameter for part2(written in bash file): [temp_filename] [MB] [output_filename]  
2.to be simple, there are 5 bash file for each rank’s size on 1MB, 2MB, 4MB, 8MB and 16MB with the bash file name run1.sh, run2.sh, run4.sh, run8.sh and run 16.sh respectively. you can find them on bash directory.
3.the output file with different name 1MB.txt, 2MB.txt, 4MB.txt, 8MB.txt and 16MB.txt respectively in output directory.