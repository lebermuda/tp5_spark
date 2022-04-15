#GIF-7104 tp5_spark
import parallel
import sequential
import time


if __name__ == '__main__':
    filename="python.org.json" # "python.org.json"  "spark.apache.org.json"  "www.fsg.ulaval.ca.json"

    d=0.85 #damping_factor
    iteration=100

    #PageRank Sequentiel
    time_seq_start=time.time()
    res_seq=sequential.sequential_pageRank(filename,iteration,d)
    time_seq_end = time.time()
    print(res_seq[:3])
    print("Time Seq : ",time_seq_end-time_seq_start," s")

    #PageRank Parallel
    time_par_start = time.time()
    res_par = parallel.parallel_pageRange(filename)
    time_par_end = time.time()
    print(res_par)
    #print("Time Seq : ", time_par_end - time_par_start, " s")




