variable "metric_list" {
  type = object({
    metric_name   = string
    log_filter    = string
    labels        = map(object({
      value_type  = string
      description = string
    }))
    label_extractors = map(string)
  })
}

variable "project_id" {
  type = string
}
