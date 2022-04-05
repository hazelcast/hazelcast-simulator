package com.hazelcast.simulator.coordinator;

public class RemoteClientTest {

//    private static final IntegrationTestOperation DEFAULT_OPERATION = new IntegrationTestOperation();
//    private static final String DEFAULT_TEST_ID = "RemoteClientTest";
//
//    private final Registry registry = new Registry();
//
//    private final CoordinatorConnector coordinatorConnector = mock(CoordinatorConnector.class);
//    private RemoteClient remoteClient;
//    private SimulatorAddress testAddress;
//
//    @Before
//    public void before() {
//        registry.addAgent("192.168.0.1", "192.168.0.1");
//        registry.addAgent("192.168.0.2", "192.168.0.2");
//        registry.addAgent("192.168.0.3", "192.168.0.3");
//
//        WorkerParameters workerProcessSettings = new WorkerParameters(
//                1, WorkerType.MEMBER, "", "", 0, new HashMap<String, String>());
//
//        SimulatorAddress agentAddress = registry.getFirstAgent().getAddress();
//        registry.addWorkers(agentAddress, Collections.singletonList(workerProcessSettings));
//
//        TestCase testCase = new TestCase(DEFAULT_TEST_ID);
//
//        TestSuite testSuite = new TestSuite();
//        testSuite.addTest(testCase);
//
//        testAddress = registry.addTests(testSuite).get(0).getAddress();
//    }
//
//    @After
//    public void after() {
//        closeQuietly(remoteClient);
//    }
//
//    @Test
//    public void testLogOnAllAgents() {
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//        remoteClient.logOnAllAgents("test");
//
//        verify(coordinatorConnector).invoke(eq(ALL_AGENTS), any(LogOperation.class));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test
//    public void testLogOnAllWorkers() {
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//        remoteClient.logOnAllWorkers("test");
//
//        verify(coordinatorConnector).invoke(eq(ALL_WORKERS), any(LogOperation.class));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test
//    public void testSendToAllAgents() {
//        initMock(ResponseType.SUCCESS);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//
//        remoteClient.invokeOnAllAgents(DEFAULT_OPERATION);
//
//        verify(coordinatorConnector).invoke(eq(ALL_AGENTS), eq(DEFAULT_OPERATION));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test
//    public void testSendToAllAgents_withFailureResponse() {
//        initMock(ResponseType.UNBLOCKED_BY_FAILURE);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//
//        remoteClient.invokeOnAllAgents(DEFAULT_OPERATION);
//
//        verify(coordinatorConnector).invoke(eq(ALL_AGENTS), eq(DEFAULT_OPERATION));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test(expected = CommandLineExitException.class)
//    public void testSendToAllAgents_withErrorResponse() {
//        initMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//
//        try {
//            remoteClient.invokeOnAllAgents(DEFAULT_OPERATION);
//        } finally {
//            verify(coordinatorConnector).invoke(eq(ALL_AGENTS), eq(DEFAULT_OPERATION));
//            verifyNoMoreInteractions(coordinatorConnector);
//        }
//    }
//
//    @Test
//    public void testSendToAllWorkers() {
//        initMock(ResponseType.SUCCESS);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//
//        remoteClient.invokeOnAllWorkers(DEFAULT_OPERATION);
//
//        verify(coordinatorConnector).invoke(eq(ALL_WORKERS), eq(DEFAULT_OPERATION));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test
//    public void testSendToAllWorkers_withFailureResponse() {
//        initMock(ResponseType.UNBLOCKED_BY_FAILURE);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//
//        remoteClient.invokeOnAllWorkers(DEFAULT_OPERATION);
//
//        verify(coordinatorConnector).invoke(eq(ALL_WORKERS), eq(DEFAULT_OPERATION));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test(expected = CommandLineExitException.class)
//    public void testSendToAllWorkers_withErrorResponse() {
//        initMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//
//        try {
//            remoteClient.invokeOnAllWorkers(DEFAULT_OPERATION);
//        } finally {
//            verify(coordinatorConnector).invoke(eq(ALL_WORKERS), eq(DEFAULT_OPERATION));
//            verifyNoMoreInteractions(coordinatorConnector);
//        }
//    }
//
//    @Test
//    public void testSendToFirstWorker() {
//        initMock(ResponseType.SUCCESS);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//        SimulatorAddress firstWorkerAddress = registry.getFirstWorker().getAddress();
//
//        remoteClient.invokeOnFirstWorker(DEFAULT_OPERATION);
//
//        verify(coordinatorConnector).invoke(eq(firstWorkerAddress), eq(DEFAULT_OPERATION));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test
//    public void testSendToFirstWorker_withFailureResponse() {
//        initMock(ResponseType.UNBLOCKED_BY_FAILURE);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//        SimulatorAddress firstWorkerAddress = registry.getFirstWorker().getAddress();
//
//        remoteClient.invokeOnFirstWorker(DEFAULT_OPERATION);
//
//        verify(coordinatorConnector).invoke(eq(firstWorkerAddress), eq(DEFAULT_OPERATION));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test(expected = CommandLineExitException.class)
//    public void testSendToFirstWorker_withErrorResponse() {
//        initMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//        SimulatorAddress firstWorkerAddress = registry.getFirstWorker().getAddress();
//
//        try {
//            remoteClient.invokeOnFirstWorker(DEFAULT_OPERATION);
//        } finally {
//            verify(coordinatorConnector).invoke(eq(firstWorkerAddress), eq(DEFAULT_OPERATION));
//            verifyNoMoreInteractions(coordinatorConnector);
//        }
//    }
//
//    @Test
//    public void testSendToTestOnAllWorkers() {
//        initMock(ResponseType.SUCCESS);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//
//        remoteClient.invokeOnTestOnAllWorkers(testAddress,DEFAULT_OPERATION);
//
//        verify(coordinatorConnector).invoke(eq(ALL_WORKERS.getChild(1)), eq(DEFAULT_OPERATION));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test
//    public void testSendToTestOnAllWorkers_withFailureResponse() {
//        initMock(ResponseType.UNBLOCKED_BY_FAILURE);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//
//        remoteClient.invokeOnTestOnAllWorkers(testAddress,DEFAULT_OPERATION);
//
//        verify(coordinatorConnector).invoke(eq(ALL_WORKERS.getChild(1)), eq(DEFAULT_OPERATION));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test(expected = CommandLineExitException.class)
//    public void testSendToTestOnAllWorkers_withErrorResponse() {
//        initMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//
//        try {
//            remoteClient.invokeOnTestOnAllWorkers(testAddress,DEFAULT_OPERATION);
//        } finally {
//            verify(coordinatorConnector).invoke(eq(ALL_WORKERS.getChild(1)), eq(DEFAULT_OPERATION));
//            verifyNoMoreInteractions(coordinatorConnector);
//        }
//    }
//
//    @Test
//    public void testSendToTestOnFirstWorker() {
//        initMock(ResponseType.SUCCESS);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//        SimulatorAddress testOnFirstWorkerAddress = registry.getFirstWorker().getAddress().getChild(1);
//
//        remoteClient.invokeOnTestOnFirstWorker(testAddress, DEFAULT_OPERATION);
//
//        verify(coordinatorConnector).invoke(eq(testOnFirstWorkerAddress), eq(DEFAULT_OPERATION));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test
//    public void testSendToTestOnFirstWorker_withFailureResponse() {
//        initMock(ResponseType.UNBLOCKED_BY_FAILURE);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//        SimulatorAddress testOnFirstWorkerAddress = registry.getFirstWorker().getAddress().getChild(1);
//
//        remoteClient.invokeOnTestOnFirstWorker(testAddress, DEFAULT_OPERATION);
//
//        verify(coordinatorConnector).invoke(eq(testOnFirstWorkerAddress), eq(DEFAULT_OPERATION));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test(expected = CommandLineExitException.class)
//    public void testSendToTestOnFirstWorker_withErrorResponse() {
//        initMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 0);
//        SimulatorAddress testOnFirstWorkerAddress = registry.getFirstWorker().getAddress().getChild(1);
//
//        try {
//            remoteClient.invokeOnTestOnFirstWorker(testAddress, DEFAULT_OPERATION);
//        } finally {
//            verify(coordinatorConnector).invoke(eq(testOnFirstWorkerAddress), eq(DEFAULT_OPERATION));
//            verifyNoMoreInteractions(coordinatorConnector);
//        }
//    }
//
//    @Test
//    public void testPingWorkerThread_shouldStopAfterInterruptedException() {
//        Response response = new Response(1L, ALL_WORKERS);
//        response.addPart(new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0), ResponseType.SUCCESS);
//
//        when(coordinatorConnector.invoke(eq(ALL_WORKERS), any(PingOperation.class)))
//                .thenThrow(new SimulatorProtocolException("expected exception", new InterruptedException()))
//                .thenReturn(response);
//
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 50);
//
//        sleepMillis(300);
//
//        remoteClient.close();
//
//        verify(coordinatorConnector).invoke(eq(ALL_WORKERS), any(PingOperation.class));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test
//    public void testPingWorkerThread_shouldContinueAfterOtherException() {
//        Response response = new Response(1L, ALL_WORKERS);
//        response.addPart(new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0), ResponseType.SUCCESS);
//
//        when(coordinatorConnector.invoke(eq(ALL_WORKERS), any(PingOperation.class)))
//                .thenThrow(new SimulatorProtocolException("expected exception", new TimeoutException()))
//                .thenReturn(response);
//
//        remoteClient = new RemoteClient(coordinatorConnector, registry, 50);
//
//        sleepMillis(300);
//
//        remoteClient.close();
//
//        verify(coordinatorConnector, atLeast(2)).invoke(eq(ALL_WORKERS), any(PingOperation.class));
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    @Test
//    public void testPingWorkerThread_shouldDoNothingIfDisabled() {
//        remoteClient = new RemoteClient(coordinatorConnector, registry, -1);
//
//        sleepMillis(300);
//
//        remoteClient.close();
//
//        verifyNoMoreInteractions(coordinatorConnector);
//    }
//
//    private void initMock(ResponseType responseType) {
//        Map<SimulatorAddress, Response.Part> parts = new HashMap<SimulatorAddress, Response.Part>();
//        parts.put(COORDINATOR, new Response.Part(responseType, null));
//
//        Response response = mock(Response.class);
//        when(response.getParts()).thenReturn(parts.entrySet());
//
//        when(coordinatorConnector.invoke(any(SimulatorAddress.class), any(SimulatorOperation.class))).thenReturn(response);
//    }
}
