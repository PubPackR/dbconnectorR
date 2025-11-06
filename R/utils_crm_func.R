conditional_unnest <- function(df, var, sep){
  if(var %in% names(df)){
    if (is.na(sep)) {
      return(unnest(df, var, keep_empty = TRUE))
    } else {
      return(unnest(df, var, names_sep = sep, keep_empty = TRUE))
    }
  } else{
    return(df)
  }
}