#include <mpi.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <vector>

static constexpr double kProductionMean = 5.0;
static constexpr double kProductionStdDev = 2.0;
static constexpr double kConsumptionMean = 5.0;
static constexpr double kConsumptionStdDev = 2.0;
static constexpr double kAccumulatorCapacity = 25.0;
static constexpr double kInitialSocRatio = 0.5;

struct Config {
  std::string strategy;
  int num_districts = 0;
  int producers_per_district = 0;
  int consumers_per_district = 0;
  int accumulators_per_district = 0;
  int timesteps = 0;
  int seed = 0;
  std::string csv_path;
};

struct DistrictState {
  int district_id = 0;
  std::vector<double> accumulator_soc; // state of charge per accumulator
};

struct StepMetrics {
  double produced = 0.0;
  double consumed = 0.0;
  double raw_balance = 0.0;      // produced - consumed
  double post_acc_balance = 0.0; // residual after accumulators
  double soc_total = 0.0;        // sum of all accumulator SoCs
};

struct RankStats {
  double min = 0.0;
  double max = 0.0;
  double avg = 0.0;
};

//   ./sim_mpi <strategy> <districts> <producers> <consumers> <accumulators>
//   <timesteps> <seed> <csv_path>

static bool parse_args(int argc, char **argv, Config &cfg) {
  if (argc != 9) {
    if (argc > 0) {
      std::cerr << "Usage: " << argv[0]
                << " <strategy> <districts> <producers_per_district>"
                   " <consumers_per_district> <accumulators_per_district>"
                   " <timesteps> <seed> <csv_output_path>\n"
                << "\n  strategy: contiguous | round_robin\n";
    }
    return false;
  }

  cfg.strategy = argv[1];
  cfg.num_districts = std::atoi(argv[2]);
  cfg.producers_per_district = std::atoi(argv[3]);
  cfg.consumers_per_district = std::atoi(argv[4]);
  cfg.accumulators_per_district = std::atoi(argv[5]);
  cfg.timesteps = std::atoi(argv[6]);
  cfg.seed = std::atoi(argv[7]);
  cfg.csv_path = argv[8];

  if (cfg.strategy != "contiguous" && cfg.strategy != "round_robin") {
    std::cerr << "Error: strategy must be 'contiguous' or 'round_robin',"
              << " got '" << cfg.strategy << "'\n";
    return false;
  }
  if (cfg.num_districts <= 0 || cfg.producers_per_district < 0 ||
      cfg.consumers_per_district < 0 || cfg.accumulators_per_district < 0 ||
      cfg.timesteps <= 0) {
    std::cerr << "Error: districts and timesteps must be > 0,"
              << " node counts must be >= 0.\n";
    return false;
  }
  return true;
}

static std::vector<int> assign_districts(int rank, int num_ranks,
                                         int num_districts,
                                         const std::string &strategy) {
  std::vector<int> owned;

  if (strategy == "round_robin") {
    for (int d = 0; d < num_districts; ++d) {
      if (d % num_ranks == rank) {
        owned.push_back(d);
      }
    }
  } else {
    const int base = num_districts / num_ranks;
    const int extra = num_districts % num_ranks;
    const int count = (rank < extra) ? (base + 1) : base;
    const int first = (rank < extra) ? rank * (base + 1) : rank * base + extra;
    owned.reserve(count);
    for (int i = 0; i < count; ++i) {
      owned.push_back(first + i);
    }
  }

  return owned;
}

static StepMetrics
simulate_district_step(DistrictState &district, int num_producers,
                       int num_consumers, std::mt19937 &rng,
                       std::normal_distribution<double> &prod_dist,
                       std::normal_distribution<double> &cons_dist) {
  StepMetrics m;

  for (int i = 0; i < num_producers; ++i)
    m.produced += std::max(0.0, prod_dist(rng));

  for (int i = 0; i < num_consumers; ++i)
    m.consumed += std::max(0.0, cons_dist(rng));

  m.raw_balance = m.produced - m.consumed;
  double residual = m.raw_balance;

  if (residual > 0.0) {

    for (double &soc : district.accumulator_soc) {
      const double room = kAccumulatorCapacity - soc;
      if (room <= 0.0)
        continue;
      const double charged = std::min(room, residual);
      soc += charged;
      residual -= charged;
      if (residual <= 1e-12) {
        residual = 0.0;
        break;
      }
    }
  } else if (residual < 0.0) {
    double needed = -residual;
    for (double &soc : district.accumulator_soc) {
      if (soc <= 0.0)
        continue;
      const double discharged = std::min(soc, needed);
      soc -= discharged;
      needed -= discharged;
      if (needed <= 1e-12) {
        needed = 0.0;
        break;
      }
    }
    residual = -needed;
  }

  m.post_acc_balance = residual;
  m.soc_total = std::accumulate(district.accumulator_soc.begin(),
                                district.accumulator_soc.end(), 0.0);
  return m;
}

static RankStats reduce_timing(double local_val, int rank, int num_ranks) {
  RankStats s;
  double sum = 0.0;
  MPI_Reduce(&local_val, &s.min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_val, &s.max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_val, &sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    s.avg = sum / static_cast<double>(num_ranks);
  }
  return s;
}

static void write_csv_row(const std::string &path, const Config &cfg,
                          int num_ranks, const RankStats &wall,
                          const RankStats &compute, const RankStats &comm,
                          double energy_produced, double energy_consumed,
                          double raw_balance, double post_acc_balance,
                          double final_soc) {
  namespace fs = std::filesystem;
  const fs::path out(path);

  if (out.has_parent_path()) {
    fs::create_directories(out.parent_path());
  }

  const bool need_header = !fs::exists(out) || fs::file_size(out) == 0;
  std::ofstream ofs(path, std::ios::app);
  if (!ofs) {
    std::cerr << "Error: cannot open CSV file '" << path << "'\n";
    return;
  }

  if (need_header) {
    ofs << "strategy,num_ranks,num_districts,"
           "producers_per_district,consumers_per_district,"
           "accumulators_per_district,timesteps,seed,total_nodes,"
           "wall_time_s,compute_time_s,comm_time_s,comm_ratio,"
           "avg_step_time_s,"
           "max_rank_wall_s,min_rank_wall_s,avg_rank_wall_s,"
           "max_rank_compute_s,avg_rank_compute_s,"
           "max_rank_comm_s,avg_rank_comm_s,"
           "load_imbalance_ratio,"
           "energy_produced_total,energy_consumed_total,"
           "raw_balance_total,post_acc_balance_total,final_soc_total\n";
  }

  const int total_nodes = cfg.num_districts * (cfg.producers_per_district +
                                               cfg.consumers_per_district +
                                               cfg.accumulators_per_district);

  const double wall_time = wall.max;
  const double compute_time = compute.max;
  const double comm_time = comm.max;
  const double comm_ratio = (wall_time > 0.0) ? (comm_time / wall_time) : 0.0;
  const double avg_step =
      (cfg.timesteps > 0) ? (wall_time / cfg.timesteps) : 0.0;
  const double imbalance = (wall.avg > 0.0) ? (wall.max / wall.avg) : 0.0;

  ofs << std::fixed << std::setprecision(6) << cfg.strategy << "," << num_ranks
      << "," << cfg.num_districts << "," << cfg.producers_per_district << ","
      << cfg.consumers_per_district << "," << cfg.accumulators_per_district
      << "," << cfg.timesteps << "," << cfg.seed << "," << total_nodes << ","
      << wall_time << "," << compute_time << "," << comm_time << ","
      << comm_ratio << "," << avg_step << "," << wall.max << "," << wall.min
      << "," << wall.avg << "," << compute.max << "," << compute.avg << ","
      << comm.max << "," << comm.avg << "," << imbalance << ","
      << energy_produced << "," << energy_consumed << "," << raw_balance << ","
      << post_acc_balance << "," << final_soc << "\n";
}

int main(int argc, char **argv) {

  MPI_Init(&argc, &argv);

  int rank = 0;
  int num_ranks = 1;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);
  Config cfg;
  if (!parse_args(argc, argv, cfg)) {
    MPI_Finalize();
    return 1;
  }

  const std::vector<int> my_districts =
      assign_districts(rank, num_ranks, cfg.num_districts, cfg.strategy);

  std::vector<DistrictState> local_states;
  local_states.reserve(my_districts.size());
  for (int did : my_districts) {
    DistrictState ds;
    ds.district_id = did;
    ds.accumulator_soc.assign(cfg.accumulators_per_district,
                              kAccumulatorCapacity * kInitialSocRatio);
    local_states.push_back(std::move(ds));
  }

  std::mt19937 rng(cfg.seed + rank * 97);
  std::normal_distribution<double> prod_dist(kProductionMean,
                                             kProductionStdDev);
  std::normal_distribution<double> cons_dist(kConsumptionMean,
                                             kConsumptionStdDev);

  double local_produced = 0.0;
  double local_consumed = 0.0;
  double local_raw_bal = 0.0;
  double local_post_bal = 0.0;
  double local_compute_time = 0.0;
  double local_comm_time = 0.0;

  MPI_Barrier(MPI_COMM_WORLD); // synchronise before timing
  const double wall_start = MPI_Wtime();

  for (int t = 0; t < cfg.timesteps; ++t) {

    const double t_comp_start = MPI_Wtime();

    for (DistrictState &ds : local_states) {
      StepMetrics m = simulate_district_step(ds, cfg.producers_per_district,
                                             cfg.consumers_per_district, rng,
                                             prod_dist, cons_dist);

      local_produced += m.produced;
      local_consumed += m.consumed;
      local_raw_bal += m.raw_balance;
      local_post_bal += m.post_acc_balance;
    }

    local_compute_time += MPI_Wtime() - t_comp_start;

    const double t_comm_start = MPI_Wtime();
    MPI_Barrier(MPI_COMM_WORLD);
    local_comm_time += MPI_Wtime() - t_comm_start;
  }

  const double local_wall_time = MPI_Wtime() - wall_start;

  double local_final_soc = 0.0;
  for (const DistrictState &ds : local_states) {
    local_final_soc += std::accumulate(ds.accumulator_soc.begin(),
                                       ds.accumulator_soc.end(), 0.0);
  }
  RankStats wall_stats = reduce_timing(local_wall_time, rank, num_ranks);
  RankStats compute_stats = reduce_timing(local_compute_time, rank, num_ranks);
  RankStats comm_stats = reduce_timing(local_comm_time, rank, num_ranks);

  double global_produced = 0.0, global_consumed = 0.0;
  double global_raw_bal = 0.0, global_post_bal = 0.0;
  double global_soc = 0.0;

  MPI_Reduce(&local_produced, &global_produced, 1, MPI_DOUBLE, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&local_consumed, &global_consumed, 1, MPI_DOUBLE, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&local_raw_bal, &global_raw_bal, 1, MPI_DOUBLE, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&local_post_bal, &global_post_bal, 1, MPI_DOUBLE, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&local_final_soc, &global_soc, 1, MPI_DOUBLE, MPI_SUM, 0,
             MPI_COMM_WORLD);

  if (rank == 0) {
    const double imbalance =
        (wall_stats.avg > 0.0) ? (wall_stats.max / wall_stats.avg) : 0.0;

    std::cout << std::fixed << std::setprecision(6) << "\n=== Run Summary ===\n"
              << "  strategy       = " << cfg.strategy << "\n"
              << "  ranks          = " << num_ranks << "\n"
              << "  districts      = " << cfg.num_districts << "\n"
              << "  timesteps      = " << cfg.timesteps << "\n"
              << "  wall_time_s    = " << wall_stats.max << "\n"
              << "  compute_time_s = " << compute_stats.max << "\n"
              << "  comm_time_s    = " << comm_stats.max << "\n"
              << "  load_imbalance = " << imbalance << "\n"
              << "  produced_total = " << global_produced << "\n"
              << "  consumed_total = " << global_consumed << "\n"
              << "  final_soc      = " << global_soc << "\n";

    write_csv_row(cfg.csv_path, cfg, num_ranks, wall_stats, compute_stats,
                  comm_stats, global_produced, global_consumed, global_raw_bal,
                  global_post_bal, global_soc);

    std::cout << "  CSV written to: " << cfg.csv_path << "\n";
  }

  MPI_Finalize();
  return 0;
}
