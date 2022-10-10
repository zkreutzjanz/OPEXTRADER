echo "~Starting Remote Code Build~"
mpic++ -x c++ -o /bin/myexec /src/topshelf.cpp /src/oanalysis.hpp /src/oio.hpp /src/ologger.hpp /src/ostat.hpp /src/ostructs.hpp
echo "~Ending Remote Code Build~"
echo "~Starting Remote Code Run~"
##mpirun -f hosts -n 16 /home/mpiuser/myexec
mpiexec -f hosts -n 16 /bin/myexec
##mpirun -f hosts -n 64 xterm -e gdb /home/mpiuser/myexec -ex run
echo "~Ending Remote Code Run~"
