# Remove existing bnstruct package and install the updated version
#remove.packages("bnstruct")
#library(devtools)
#install_git("https://github.com/azar2020/bnstruct")

# Set working directory
setwd("C:/Azar_Drive/relationships-between-variables1/01_preprocessing/results")

# Define number of rounding digits
options(digits = 2)

# Required libraries
library(bnlearn)
library(bnstruct)
library(Rgraphviz)

# Read data
Creel_app_data_daily_agg <- read.csv("Creel_app_data_daily_aggregated.csv")
Creel_app_data_daily_agg <- Creel_app_data_daily_agg[, c("creel_average_fishing_time", "creel_mean_catch_rate",
                                                         "air_temperature", "solar_radiation", "relative_humidity", 
                                                         "degree_days", "wind_speed_at_2_meters", "app_average_fishing_time",
                                                         "app_mean_catch_rate", "total_precipitation", "unique_page_views", 
                                                         "is_weekend")]

# Data discretization
data_quantile_discretized_variables <- Creel_app_data_daily_agg[, c("creel_average_fishing_time", "air_temperature", 
                                                                    "creel_mean_catch_rate", "solar_radiation", 
                                                                    "relative_humidity", "degree_days", 
                                                                    "wind_speed_at_2_meters"), drop = FALSE]
data_quantile_discretized_variables <- lapply(data_quantile_discretized_variables, as.numeric)
data_quantile_discretized_variables <- as.data.frame(data_quantile_discretized_variables) 

discretized_data_quantile <- discretize(data_quantile_discretized_variables, method = 'quantile', breaks = 3)

# Discretize app_average_fishing_time
variable_values_1 <- Creel_app_data_daily_agg$app_average_fishing_time
non_zero_values_1 <- variable_values_1[variable_values_1 > 0]
quantile_breaks_1 <- quantile(non_zero_values_1, probs = c(0, 1/3, 2/3, 1))
bin_labels_1 <- c("[0,0.5)", "(0.5, 0.9]", "(1, 1]")
discretized_values_1 <- cut(variable_values_1, breaks = 3, labels = bin_labels_1, include.lowest = TRUE)
discretized_values_1[variable_values_1 == 0] <- bin_labels_1[1]
Creel_app_data_daily_agg["app_average_fishing_time"] <- discretized_values_1

# Repeat the above steps for other variables

# Combine all discretized variables
Discretized_data <- cbind(discretized_data_quantile,
                          Creel_app_data_daily_agg[c("app_average_fishing_time", "app_mean_catch_rate", 
                                                     "total_precipitation", "unique_page_views", "is_weekend")])

# Convert discretized_data to dataframe 
Discretized_data <- as.data.frame(Discretized_data)

# Convert discretized_data to numeric            
Creel_app_data_daily_agg <- sapply(Discretized_data, as.numeric)   

# Save the discretized data
write.csv(Creel_app_data_daily_agg, "C:/Azar_Drive/relationships-between-variables1/01_preprocessing/results/Discretized_Creel_app_data_daily_agg.csv", row.names = FALSE)

# Create BNDataset object
BNDataset_object <- BNDataset(Creel_app_data_daily_agg, discreteness = rep("d3", 12), 
                              variables = c("creel_average_fishing_time", "creel_mean_catch_rate", 
                                            "air_temperature", "solar_radiation", "relative_humidity", 
                                            "degree_days", "wind_speed_at_2_meters", "app_average_fishing_time", 
                                            "app_mean_catch_rate", "total_precipitation", "unique_page_views", "is_weekend"), 
                              node.sizes = rep(3, 12))

# Learn the structure (graph) of BN using Silander Myllymaki (sm) global search and BIC score
BN_structure <- learn.network(BNDataset_object, algo = "sm", scoring.func = "BIC")

# Plot the graph
plot(BN_structure, plot.wpdag = TRUE, node.col = c("coral", "coral", "aquamarine3", "aquamarine3", "aquamarine3", 
                                                   "aquamarine3", "aquamarine3", "orange1", "orange1", "aquamarine3", "gray", "aquamarine3"),
     node.lab.cex = 4.5)

# Compute BIC
adjacency_matrix <- dag(BN_structure)
rownames(adjacency_matrix) <- names(Creel_app_data_daily_agg)
colnames(adjacency_matrix) <- names(Creel_app_data_daily_agg)

# Create Bayesian network object
bn_structure <- empty.graph(nodes = names(Creel_app_data_daily_agg))
for (i in 1:nrow(adjacency_matrix)) {
  for (j in 1:ncol(adjacency_matrix)) {
    if (adjacency_matrix[i, j] != 0) {
      bn_structure <- set.arc(bn_structure, from = names(Creel_app_data_daily_agg)[i], to = names(Creel_app_data_daily_agg)[j])
    }
  }
}

node_sizes <- node.sizes(BN_structure)
bn_object <- bn.fit(bn_structure, data = data.frame(Creel_app_data_daily_agg), node.sizes = node_sizes, discrete = names(Creel_app_data_daily_agg), extra = cpts)
BIC_value <- BIC(bn_object, data.frame(Creel_app_data_daily_agg))
print(BIC_value)
