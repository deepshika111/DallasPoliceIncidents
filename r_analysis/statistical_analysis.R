suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(scales)
})

args <- commandArgs(trailingOnly = FALSE)
file_arg <- grep("^--file=", args, value = TRUE)
script_dir <- if (length(file_arg) > 0) {
  dirname(normalizePath(sub("^--file=", "", file_arg[1])))
} else {
  getwd()
}

root_dir <- normalizePath(file.path(script_dir, ".."))
clean_dir <- file.path(root_dir, "data", "clean")
output_dir <- file.path(root_dir, "outputs")

dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

crimes <- read_csv(file.path(clean_dir, "crimes_clean.csv"), show_col_types = FALSE)
division_stats <- read_csv(file.path(clean_dir, "division_stats.csv"), show_col_types = FALSE)
division_trends <- read_csv(file.path(clean_dir, "division_trends.csv"), show_col_types = FALSE)

crime_table <- table(crimes$crime_category, crimes$year)
chi_result <- chisq.test(crime_table)
print(chi_result)

anova_result <- aov(total_crimes ~ division, data = division_stats)
print(summary(anova_result))

regression_model <- lm(total_crimes ~ year + violent_rate + nighttime_rate, data = division_stats)
print(summary(regression_model))

division_plot <- division_stats %>%
  ggplot(aes(x = year, y = total_crimes, color = division, group = division)) +
  geom_line(linewidth = 1.1) +
  geom_point(size = 2) +
  labs(
    title = "Dallas Crime Trends by Division",
    x = "Year",
    y = "Total Incidents",
    color = "Division"
  ) +
  theme_minimal()

ggsave(file.path(output_dir, "r_division_trends.png"), division_plot, width = 10, height = 6)

crime_mix_plot <- crimes %>%
  count(year, crime_category) %>%
  ggplot(aes(x = factor(year), y = n, fill = crime_category)) +
  geom_col(position = "fill") +
  scale_y_continuous(labels = percent) +
  labs(
    title = "Crime Category Mix by Year",
    x = "Year",
    y = "Percentage",
    fill = "Category"
  ) +
  theme_minimal()

ggsave(file.path(output_dir, "r_crime_mix.png"), crime_mix_plot, width = 10, height = 6)

latest_year <- max(division_stats$year, na.rm = TRUE)
risk_plot <- division_stats %>%
  filter(year == latest_year) %>%
  ggplot(aes(x = reorder(division, risk_score), y = risk_score, fill = risk_score)) +
  geom_col() +
  coord_flip() +
  scale_fill_gradient(low = "#2f7d32", high = "#c62828") +
  labs(
    title = paste("Dallas Division Risk Scores", latest_year),
    x = "Division",
    y = "Risk Score"
  ) +
  theme_minimal()

ggsave(file.path(output_dir, "r_risk_scores.png"), risk_plot, width = 8, height = 5)

cat("\nTrend snapshot\n")
print(division_trends %>% select(division, trend_label, crime_change_pct, risk_score))
