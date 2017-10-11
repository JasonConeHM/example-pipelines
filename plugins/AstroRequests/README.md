# Astro Request (In Development)

This is an ongoing attempt to generalize request calls in the form of an Airflow Operator and Hook.

Features
    [X] Send request to Xcom
    [X] Send request to S3
    [X] Use Xcom values as params in a new request (Request Chaining)
    [...] Pagination (Still in development, lots of edge cases)
        [...] Autopilot pagination support (In Progress) 
    [...] Support Various Types of Auth
        [X] Basic Auth
        [...] Token Auth
    [] Rate limiting (Not started)