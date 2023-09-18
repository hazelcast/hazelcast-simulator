# Hazelcast Simulator Scenarios (CP)

Following the steps in these instructions yields the following topology:

```
┌──────────────────────────────────────┐
│  Members (c5.4xlarge; Xms/Xmx=24g)   │
├──────────────────────────────────────┴──────────────────────────┐
│┌───────┐             ┌───────┐             ┌───────┐            │
││  VM   │             │  VM   │             │  VM   │            │
│├───────┴──────────┐  ├───────┴──────────┐  ├───────┴──────────┐ │
││      Server      │  │      Server      │  │      Server      │ │
│└──────────────────┘  └──────────────────┘  └──────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                 ▲
                                 ┃
                    ┏━━━━━━━━━━━━┛
                    ┃
                    ▼
┌──────────────────────────────────────┐
│   Clients (c5.4xlarge; Xms/Xmx=24g)  │
├──────────────────────────────────────┴──────────────────────────┐
│┌───────┐             ┌───────┐             ┌───────┐            │
││  VM   │             │  VM   │             │  VM   │            │
│├───────┴──────────┐  ├───────┴──────────┐  ├───────┴──────────┐ │
││      Client      │  │      Client      │  │      Client      │ │
│└──────────────────┘  └──────────────────┘  └──────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

If you want to monitor JVM memory etc then you can follow the instructions
[here](https://github.com/gbarnett-hz/hazelcast-simulator#profiling-your-simulator-test).

## Usage

Create a performance test project as normal, `perftest create myproject`, then you will want to
overwrite some of the files in that default project with the following:

- `hazelcast.xml`

Edit `inventory_plan.yaml` so that `nodes.count=3` and `loadgenerators.count=3`.

_Note._ I would also remove `tests.yaml` from the `myproject` directory as well just to remove
confusion.

Now, copy the `*.yaml` files to `myproject`. After you have spun up the environment according to the
`README.md` in the `myproject` directory you can then do the following (from `myproject`), e.g.

```bash
> perftest run iatomicreference.yaml
```

Keep an eye on the distribution of clients and members -- they're on distinct VMs and categories, if
in doubt check the log messages that are printed near the beginning of each test, e.g. 3-member,
1-client:

```
INFO  11:30:20     Agent  18.194.240.208      10.0.77.24 (A1) members:  1, clients:  0, mode: MEMBERS_ONLY, version specs: [maven=5.1]
INFO  11:30:20     Agent   3.127.136.249      10.0.77.27 (A2) members:  1, clients:  0, mode: MEMBERS_ONLY, version specs: [maven=5.1]
INFO  11:30:20     Agent      3.77.202.6     10.0.77.178 (A3) members:  1, clients:  0, mode: MEMBERS_ONLY, version specs: [maven=5.1]
INFO  11:30:20     Agent   18.159.131.86     10.0.77.166 (A4) members:  0, clients:  1, mode: CLIENTS_ONLY, version specs: [maven=5.1]
INFO  11:30:20     Agent    3.70.133.150      10.0.77.13 (A5) members:  0, clients:  0 (no workers)
INFO  11:30:20     Agent     3.79.107.96     10.0.77.231 (A6) members:  0, clients:  0 (no workers)
```

I've kept each test running for 120 seconds (2 mins).

I would look at the source code for the tests themselves and what they do and importantly would the
different probabilities changes in the actual configurations.

Most likely you will want to compare all test scenarios against each other, e.g. test-1:
1client-3member vs 2client-3member vs 3client-3member. Please see
[here](https://github.com/gbarnett-hz/hazelcast-simulator#generate-comparison-reports) for how to do
this.
