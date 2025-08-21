#!/bin/bash
#SBATCH --job-name=cstar_job_20250804_101530
#SBATCH --output=/pscratch/sd/e/eilerman/2node1wk/playground/output/cstar_job_20250804_101530.out
#SBATCH --qos=debug
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --account=m4632
#SBATCH --export=ALL
#SBATCH --mail-type=ALL
#SBATCH --time=00:05:00
#SBATCH -C cpu

export PODMANHPC_WAIT_TIMEOUT=90
# export PODMANHPC_PODMAN_BIN=/global/common/shared/das/podman-4.7.0/bin/podman

# export SLURM_MPI_TYPE=pmi2
export IMG_NAME=60d27c0d6fb3

module load openmpi/5.0.7

# srun -n 256 --mpi=pmi2 podman-hpc run --rm -v /pscratch/sd/e/eilerman/2node1wk/playground:/work --openmpi-pmi2 $IMG_NAME  bash /entrypoint-run.sh

# podman-hpc run --rm -v /pscratch/sd/e/eilerman/2node1wk/playground:/work --openmpi-pmix $IMG_NAME  bash /entrypoint-run.sh


# podman-hpc run --rm -v /pscratch/sd/e/eilerman/2node1wk/playground:/work --openmpi-pmix $IMG_NAME mpiexec /opt/roms /work/ROMS/runtime_code/2node_test.in

### iterated on this one with chris
# srun --cpus-per-task=128 --mpi=pmix podman-hpc run  --rm -v /pscratch/sd/e/eilerman/2node1wk/playground:/work --openmpi-pmix $IMG_NAME bash -c "source /etc/profile &&  mpirun /opt/work_internal/compile_time_code/roms /work/ROMS/runtime_code/2node_test.in"

# srun -n 256 --mpi=pmi2 podman-hpc run --userns keep-id --rm -v /pscratch/sd/e/eilerman/2node1wk/playground:/work --openmpi-pmi2 $IMG_NAME bash -c "source /etc/profile && unset SLURM_JOBID && mpirun /opt/work_internal/compile_time_code/roms /work/ROMS/runtime_code/2node_test.in"
# podman-hpc run --userns keep-id --rm -v /pscratch/sd/e/eilerman/2node1wk/playground:/work --openmpi-pmi2 $IMG_NAME source /etc/profile & mpirun /opt/roms /work/ROMS/runtime_code/2node_test.in

# && unset SLURM_JOBID
srun -N 1 --mpi=pmi2 podman-hpc run  --rm -v /pscratch/sd/e/eilerman/2node1wk/playground:/work --openmpi-pmi2 $IMG_NAME bash -c "source /etc/profile && echo $SLURM_NODELIST  && mpirun --host $SLURM_NODELIST -npernode 128 --display-topo --display-devel-allocation /opt/work_internal/compile_time_code/roms /work/ROMS/runtime_code/2node_test.in"

# srun -N 1 podman-hpc run --userns keep-id --rm -v /pscratch/sd/e/eilerman/2node1wk/playground:/work --openmpi-pmi2 $IMG_NAME bash -c "source /etc/profile && unset SLURM_JOBID && mpirun /opt/work_internal/compile_time_code/roms /work/ROMS/runtime_code/2node_test.in"

# srun -N 1 --mpi=$SLURM_MPI_TYPE podman-hpc run --userns keep-id --rm -v /pscratch/sd/e/eilerman/2node1wk/playground:/work --openmpi-$SLURM_MPI_TYPE $IMG_NAME bash -c "source /etc/profile && unset SLURM_JOBID && mpirun -n 128 /opt/work_internal/compile_time_code/roms /work/ROMS/runtime_code/2node_test.in"