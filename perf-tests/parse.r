library("ggplot2")
library("rstudioapi")
library("tidyverse")

###################
# stat_myecdf <- function(mapping = NULL, data = NULL, geom = "step", position = "identity", n = NULL,
#                         na.rm = FALSE, show.legend = NA, inherit.aes = TRUE, direction="vh", ...) {
#   layer(
#     data = data,
#     mapping = mapping,
#     stat = StatMyecdf,
#     geom = geom,
#     position = position,
#     show.legend = show.legend,
#     inherit.aes = inherit.aes,
#     params = list(
#       n = n,
#       na.rm = na.rm,
#       direction=direction,
#       ...
#     )
#   )
# }
#
# StatMyecdf <- ggproto("StatMyecdf", Stat,
#   compute_group = function(data, scales, n = NULL) {
#     # If n is NULL, use raw values; otherwise interpolate
#     if (is.null(n)) {
#       # Dont understand why but this version needs to sort the values
#       xvals <- sort(unique(data$x))
#     } else {
#       xvals <- seq(min(data$x), max(data$x), length.out = n)
#     }
#     y <- ecdf(data$x)(xvals)
#     x1 <- max(xvals)
#     y0 <- 0
#     data.frame(x = c(xvals, x1), y = c(y0, y))
#   },
#   default_aes = aes(y = ..y..),
#   required_aes = c("x")
# )
###################


# Automatically set working directory
#current_path <- getActiveDocumentContext()$path
#setwd(dirname(current_path ))

# Operate on a 9 digits precision
options(digits=9)
options(scipen=9)

# The logs to read for 8K batches
read_dir <- function(file_name) {
  read_csv(file_name, col_types = cols(
    .default = col_guess(),
    name = col_factor(),
    size = col_integer(),
    msgs = col_integer())
    )
}

logdir <- c(".");
logfiles <- c();
for(l in logdir) {
  for(d in list.files(l, full.names = TRUE)) {
    logfiles <- c(logfiles, list.files(d, pattern = "*.csv$", full.names=TRUE));
  }
}

erdos_tcp <- read_csv("erdos-tcp.csv")
erdos_zenoh <- read_csv("erdos-zenoh.csv")

log <- bind_rows(erdos_tcp, erdos_zenoh) %>% filter(msgs != "")

# Read the file and remove the first and last observation of each group

data <- log %>%
  group_by(size,kind,scenario) %>%
  summarise(msgs = median(msgs)) %>%
  mutate(throughput = 8 * size * msgs) 

ggplot(log,
       aes(x = factor(size), y = msgs, colour = kind)) +
  geom_boxplot() +
  geom_line(data = data, aes(group = kind)) +
  labs(title="localhost", x ="Message size (Bytes)", y = "msg/s") +
  scale_color_discrete(breaks=c(
    "erdos-tcp",
    "erdos-zenoh")) +
  theme_bw()
ggsave("localhost_msgps_mqtt.png", width = 12, height = 6)

ggplot(log,
       aes(x = factor(size), y = 8 * msgs * size / 1e6, colour = kind)) +
  geom_boxplot() +
  geom_line(data = data, aes(group = kind)) +
  labs(title="localhost", x ="Message size (Bytes)", y = "Mb/s") +
  scale_color_discrete(breaks=c(
    "erdos-tcp",
    "erdos-zenoh")) +
  theme_bw()
ggsave("localhost_gbps.png", width = 12, height = 6)


#
#
#
# data <- log %>%
#   filter(scenario == "localhost") %>%
#   filter(layer == "zenoh-net" | layer == "zenoh") %>%
#   group_by(size,layer,scenario) %>%
#   summarise(messages = median(messages)) %>%
#   mutate(throughput = 8 * size * messages)
# ggplot(log %>%
#          filter(scenario == "localhost") %>%
#          filter(layer == "zenoh-net" | layer == "zenoh"),
#        aes(x = factor(size), y = messages, colour = layer)) +
#   geom_boxplot() +
#   geom_line(data = data, aes(group = layer)) +
#   labs(title="P2P - localhost", x ="Message size (Bytes)", y = "msg/s") +
#   scale_color_discrete(breaks=c(
#     "session",
#     "router",
#     "zenoh-net",
#     "zenoh")) +
#   theme_bw()
# ggsave("localhost_msgps_api.png", width = 12, height = 6)
#
# ggplot(log %>%
#          filter(scenario == "localhost") %>%
#          filter(layer == "zenoh-net" | layer == "zenoh"),
#        aes(x = factor(size), y = 8 * messages * size / 1e9, colour = layer)) +
#   geom_boxplot() +
#   geom_line(data = data, aes(group = layer)) +
#   labs(title="P2P - localhost", x ="Message size (Bytes)", y = "Gb/s") +
#   scale_color_discrete(breaks=c(
#     "session",
#     "router",
#     "zenoh-net",
#     "zenoh")) +
#   theme_bw()
# ggsave("localhost_gbps_api.png", width = 12, height = 6)
#
