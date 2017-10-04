# Plugin - DummySensorOperator
This plugin is a sensor operator that returns what you tell it to. Originally conceived
to replicate an Airflow bug, it can be useful to creating example or test DAGs.,

## Operators
### DummySensorOperator
A sensor operator that is fully configurable and succeeds or fails depending
on what argument you pass into the flag argument.

`flag`: A truthy of falsey value that dictates how the sensor will evaluate.

It will also except any arguments from the BaseSensorOperator.