source(
  here::here("scripts", "complexity_analysis", "98-big_o_estimation.R"),
  echo = FALSE
)

run_big_o_analysis(
  cfg = utils::modifyList(
    get_big_o_config(),
    list(
      input_sizes = c(
        100L,
        1000L,
        10000L,
        100000L
      ),
      n_reps = 100L,
      output_dir = "~/big_o_out"
    )
  )
)
