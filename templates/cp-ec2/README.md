# Hazelcast Simulator Scenarios (CP)

See the `test-*.yaml` files for test specifics. Note that you will need to modify the
`inventory_plan.yaml` to allocate the appropriate number of VMs per-test; same for `hazelcast.yaml`
w.r.t. the CP member count. Everything else runs as documented
[here](https://github.com/hazelcast/hazelcast-simulator/blob/master/README.md). A quick example:

```bash
perftest run test-3member-iatomicreference-128kb-set-alter-cas-casopt.yaml
```
