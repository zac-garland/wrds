library(tidyverse)
# input_df <- clipr::read_clip_tbl() %>% as_tibble()


calc_lw_cov <- function(ret_mat) {
  beta_calc <- function(row_vals) {
    outer_prod <- outer(row_vals, row_vals)
    sum((outer_prod - cov_mat)^2)
  }

  cov_mat <- cov(ret_mat)
  emat <- as.matrix(ret_mat %>% mutate_all(funs(. - mean(., na.rm = TRUE))))
  n <- nrow(ret_mat)
  p <- ncol(ret_mat)
  avg_var <- mean(diag(cov_mat))
  F_mat <- diag(p) * avg_var

  beta_sum <- emat %>% as_tibble() %>%
    mutate(beta = pmap_dbl(., ~beta_calc(c(...)))) %>%
    pull(beta) %>%
    sum(na.rm = TRUE)

  beta_sq <- beta_sum / (n^2)
  delta_sq <- sum((cov_mat - F_mat)^2, na.rm = TRUE)
  lambda <- min(1, beta_sq / delta_sq)

  lambda * F_mat + (1 - lambda) * cov_mat
}

calc_gmv_weights <- function(lwcovmat) {
  ones <- rep(1, ncol(lwcovmat))
  lw_inv <- solve(lwcovmat)
  (lw_inv %*% ones) / as.numeric(t(ones) %*% lw_inv %*% ones)
}

calc_orp_weights <- function(lwcovmat, mu_vec, rf = 0.035/12) {
  excess_ret <- as.numeric(mu_vec) - rf
  lw_inv <- solve(lwcovmat)
  ones <- rep(1, ncol(lwcovmat))

  num <- lw_inv %*% excess_ret
  denom <- as.numeric(t(ones) %*% lw_inv %*% excess_ret)

  num / denom
}

portfolio_weights <- function(input_data, rf = 0.035/12) {
  ret_mat <- if("Date" %in% names(input_data)) {
    input_data %>% select(-Date)
  } else {
    input_data
  }

  mu_vec <- ret_mat %>% summarize_all(funs(mean(., na.rm = TRUE)))
  lwcovmat <- calc_lw_cov(ret_mat)

  gmv_weights <- calc_gmv_weights(lwcovmat)
  orp_weights <- calc_orp_weights(lwcovmat, mu_vec, rf)

  tibble(
    asset = names(ret_mat),
    gmv_weight = as.vector(gmv_weights),
    orp_weight = as.vector(orp_weights)
  )
}

# Usage:
portfolio_weights(input_df)








# beta_calc <- function(row_vals) {
#   outer_prod <- outer(row_vals, row_vals)
#   sum((outer_prod - cov_mat)^2)
# }
#
#
# ret_mat <- input_df %>% select(-Date)
# cov_mat <- cov(ret_mat)
# inv_cov_mat <- solve(cov_mat)
# emat <- as.matrix(ret_mat %>% mutate_all(funs(. - mean(., na.rm = TRUE))))
# mu_vec <- ret_mat %>% mutate_all(funs(var(., na.rm = TRUE))) %>% rowMeans()
# n <- nrow(ret_mat)
# mu_vec <- ret_mat %>% summarize_all(funs(mean(., na.rm = TRUE)))
# ident_mat <- diag(ncol(ret_mat)) * avg_var
# p <- ncol(ret_mat)
# avg_var <- mean(diag(cov_mat))
# F_mat <- diag(p) * avg_var
#
# beta_sum <- emat %>% as_tibble() %>%
#   mutate(beta = pmap_dbl(., ~beta_calc(c(...)))) %>% pull(beta) %>%
#   sum(na.rm = TRUE)
#
# beta_sq <- beta_sum/(n^2)
# delta_sq <- sum((cov_mat-F_mat)^2,na.rm=TRUE)
# lambda <- min(1,beta_sq/delta_sq)
#
# lwcovmat <- lambda*F_mat + (1-lambda)*cov_mat
# ones <- rep(1, p)
# lw_inv <- solve(lwcovmat)
# gmv_weights <- (lw_inv %*% ones) / as.numeric(t(ones) %*% lw_inv %*% ones)
#
# weights_df <- tibble(asset = names(ret_mat), weight = as.vector(gmv_weights))
#
# weights_df
#
#
# lw_gmv_weights <- function(input_data) {
#
#   beta_calc <- function(row_vals) {
#     outer_prod <- outer(row_vals, row_vals)
#     sum((outer_prod - cov_mat)^2)
#   }
#
#   ret_mat <- if("Date" %in% names(input_data)) {
#     input_data %>% select(-Date)
#   } else {
#     input_data
#   }
#
#   cov_mat <- cov(ret_mat)
#   emat <- as.matrix(ret_mat %>% mutate_all(funs(. - mean(., na.rm = TRUE))))
#   n <- nrow(ret_mat)
#   p <- ncol(ret_mat)
#   avg_var <- mean(diag(cov_mat))
#   F_mat <- diag(p) * avg_var
#
#   beta_sum <- emat %>% as_tibble() %>%
#     mutate(beta = pmap_dbl(., ~beta_calc(c(...)))) %>%
#     pull(beta) %>%
#     sum(na.rm = TRUE)
#
#   beta_sq <- beta_sum / (n^2)
#   delta_sq <- sum((cov_mat - F_mat)^2, na.rm = TRUE)
#   lambda <- min(1, beta_sq / delta_sq)
#   lwcovmat <- lambda * F_mat + (1 - lambda) * cov_mat
#
#   ones <- rep(1, p)
#   lw_inv <- solve(lwcovmat)
#   gmv_weights <- (lw_inv %*% ones) / as.numeric(t(ones) %*% lw_inv %*% ones)
#
#   tibble(asset = names(ret_mat), weight = as.vector(gmv_weights))
# }
#
# # Usage:
# lw_gmv_weights(ret_mat)
