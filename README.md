Co-occurrence-in-a-large-dataset
================================

Goal: Given a target word, identify which words in a body of text are most closely related to it by ranking each unique word in the corpus by its co-occurrence rate, determined using a given co-occurrence rate algorithm, with the target word.

-- Implemented MapReduce jobs in Java, which calculate co-occurrence of a target word in a large dataset.

-- Ran MapReduce on several datasets stored on Amazon’s Simple Storage Service (S3). Used Amazon’s EC2 service, which rents virtual machines by the hour, by starting up a Hadoop cluster.
