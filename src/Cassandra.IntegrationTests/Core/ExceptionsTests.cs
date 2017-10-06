//
//      Copyright (C) 2012-2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System.Diagnostics;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using Cassandra.Tests;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class ExceptionsTests : TestGlobals
    {
        private ITestCluster _testCluster;
        private ISession _session;
        private const string KeyspaceNameWriteFailName = "ks_wfail";
        private string _keyspace;
        private string _table;

        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            _testCluster = TestClusterManager.CreateNew(2, new TestClusterOptions
            {
                CassandraYaml = new [] {"enable_user_defined_functions: true", "tombstone_failure_threshold: 1000" },
                JvmArgs = new [] { "-Dcassandra.test.fail_writes_ks=" + KeyspaceNameWriteFailName }
            }, false);
        }

        [SetUp]
        public void RestartCluster()
        {
            _testCluster.Start();
            _testCluster.InitClient();
            _session = _testCluster.Session;
            _keyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            _table = TestUtils.GetUniqueTableName().ToLowerInvariant();
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            _testCluster.Remove();
        }

        /// <summary>
        ///  Tests the AlreadyExistsException. Create a keyspace twice and a table twice.
        ///  Catch and test all the exception methods.
        /// </summary>
        [Test]
        public void AlreadyExistsException()
        {   
            var cqlCommands = new []
            {
                string.Format(TestUtils.CreateKeyspaceSimpleFormat, _keyspace, 1),
                string.Format("USE \"{0}\"", _keyspace),
                string.Format(TestUtils.CreateTableSimpleFormat, _table)
            };

            // Create the schema once
            _session.Execute(cqlCommands[0]);
            _session.Execute(cqlCommands[1]);
            _session.Execute(cqlCommands[2]);

            // Try creating the keyspace again
            var ex = Assert.Throws<AlreadyExistsException>(() => _session.Execute(cqlCommands[0]));
            Assert.AreEqual(ex.Keyspace, _keyspace);
            Assert.AreEqual(ex.Table, null);
            Assert.AreEqual(ex.WasTableCreation, false);

            _session.Execute(cqlCommands[1]);

            // Try creating the table again
            try
            {
                _session.Execute(cqlCommands[2]);
            }
            catch (AlreadyExistsException e)
            {
                Assert.AreEqual(e.Keyspace, _keyspace);
                Assert.AreEqual(e.Table, _table);
                Assert.AreEqual(e.WasTableCreation, true);
            }
        }

        /// <summary>
        ///  Tests the NoHostAvailableException. by attempting to build a cluster using
        ///  the IP address "255.255.255.255" and test all available exception methods.
        /// </summary>
        [Test]
        public void NoHostAvailableException()
        {
            var ipAddress = "255.255.255.255";
            var errorsHashMap = new Dictionary<IPAddress, Exception>();
            errorsHashMap.Add(IPAddress.Parse(ipAddress), null);

            try
            {
                Cluster.Builder().AddContactPoint(ipAddress).Build();
            }
            catch (NoHostAvailableException e)
            {
                Assert.AreEqual(e.Message, string.Format("All host tried for query are in error (tried: {0})", ipAddress));
                Assert.AreEqual(e.Errors.Keys.ToArray(), errorsHashMap.Keys.ToArray());
            }
        }

        /// <summary>
        ///  Tests the ReadTimeoutException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then forcibly kill single node and attempt a read of
        ///  the key at CL.ALL. Catch and test all available exception methods.
        /// </summary>
        [Test]
        public void ReadTimeoutException()
        {
            var replicationFactor = 2;
            var key = "1";

            _session.Execute(string.Format(TestUtils.CreateKeyspaceSimpleFormat, _keyspace, replicationFactor));
            Thread.Sleep(5000);
            _session.ChangeKeyspace(_keyspace);
            _session.Execute(string.Format(TestUtils.CreateTableSimpleFormat, _table));
            Thread.Sleep(3000);

            _session.Execute(
                new SimpleStatement(string.Format(TestUtils.INSERT_FORMAT, _table, key, "foo", 42, 
                    24.03f.ToString(CultureInfo.InvariantCulture))).SetConsistencyLevel(ConsistencyLevel.All));
            _session.Execute(new SimpleStatement(string.Format(TestUtils.SELECT_ALL_FORMAT, _table)).SetConsistencyLevel(ConsistencyLevel.All));

            _testCluster.StopForce(2);
            var ex = Assert.Throws<ReadTimeoutException>(() => 
                _session.Execute(new SimpleStatement(string.Format(TestUtils.SELECT_ALL_FORMAT, _table)).SetConsistencyLevel(ConsistencyLevel.All)));
            Assert.AreEqual(ex.ConsistencyLevel, ConsistencyLevel.All);
            Assert.AreEqual(ex.ReceivedAcknowledgements, 1);
            Assert.AreEqual(ex.RequiredAcknowledgements, 2);
            Assert.AreEqual(ex.WasDataRetrieved, true);
        }

        /// <summary>
        ///  Tests SyntaxError. Tests basic message and copy abilities.
        /// </summary>
        [Test]
        public void SyntaxError()
        {
            const string errorMessage = "Test Message";

            try
            {
                throw new SyntaxError(errorMessage);
            }
            catch (SyntaxError e)
            {
                Assert.AreEqual(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests TraceRetrievalException. Tests basic message.
        /// </summary>
        [Test]
        public void TraceRetrievalException()
        {
            const string errorMessage = "Test Message";

            try
            {
                throw new TraceRetrievalException(errorMessage);
            }
            catch (TraceRetrievalException e)
            {
                Assert.AreEqual(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests TruncateException. Tests basic message and copy abilities.
        /// </summary>
        [Test]
        public void TruncateException()
        {
            const string errorMessage = "Test Message";

            try
            {
                throw new TruncateException(errorMessage);
            }
            catch (TruncateException e)
            {
                Assert.AreEqual(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests UnauthorizedException. Tests basic message and copy abilities.
        /// </summary>
        [Test]
        public void UnauthorizedException()
        {
            const string errorMessage = "Test Message";

            try
            {
                throw new UnauthorizedException(errorMessage);
            }
            catch (UnauthorizedException e)
            {
                Assert.AreEqual(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests the UnavailableException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then kill single node, wait for gossip to propogate the
        ///  new state, and attempt to read and write the same key at CL.ALL. Catch and
        ///  test all available exception methods.
        /// </summary>
        [Test]
        public void UnavailableException()
        {
            var replicationFactor = 2;
            var key = "1";
            _session.Execute(string.Format(TestUtils.CreateKeyspaceSimpleFormat, _keyspace, replicationFactor));
            _session.ChangeKeyspace(_keyspace);
            _session.Execute(string.Format(TestUtils.CreateTableSimpleFormat, _table));

            _session.Execute(
                new SimpleStatement(string.Format(TestUtils.INSERT_FORMAT, _table, key, "foo", 42, 
                24.03f.ToString(CultureInfo.InvariantCulture))).SetConsistencyLevel(ConsistencyLevel.All));
            _session.Execute(new SimpleStatement(string.Format(TestUtils.SELECT_ALL_FORMAT, _table)).SetConsistencyLevel(ConsistencyLevel.All));

            _testCluster.StopForce(2);
            // Ensure that gossip has reported the node as down.

            var expectedExceptionWasCaught = false;
            var readTimeoutWasCaught = 0;
            var maxReadTimeouts = 6; // as long as we're getting Read Timeouts, then we're on the right track

            while (!expectedExceptionWasCaught && readTimeoutWasCaught < maxReadTimeouts)
            {
                try
                {
                    _session.Execute(new SimpleStatement(string.Format(TestUtils.SELECT_ALL_FORMAT, _table)).SetConsistencyLevel(ConsistencyLevel.All));
                }
                catch (UnavailableException e)
                {
                    Assert.AreEqual(e.Consistency, ConsistencyLevel.All);
                    Assert.AreEqual(e.RequiredReplicas, replicationFactor);
                    Assert.AreEqual(e.AliveReplicas, replicationFactor - 1);
                    expectedExceptionWasCaught = true;
                }
                catch (ReadTimeoutException e)
                {
                    Assert.AreEqual(e.ConsistencyLevel, ConsistencyLevel.All);
                    Trace.TraceInformation("We caught a ReadTimeoutException as expected, extending the total time to wait ... ");
                    readTimeoutWasCaught++;
                }
                Trace.TraceInformation("Expected exception was not thrown, trying again ... ");
            }

            Assert.True(expectedExceptionWasCaught,
                string.Format("Expected exception {0} was not caught after {1} read timeouts were caught!", "UnavailableException", maxReadTimeouts));
        }

        /// <summary>
        ///  Tests the WriteTimeoutException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then forcibly kill single node and attempt to write the
        ///  same key at CL.ALL. Catch and test all available exception methods.
        /// </summary>
        [Test]
        public void WriteTimeoutException()
        {
            var replicationFactor = 2;
            var key = "1";

            _session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, _keyspace, replicationFactor));
            _session.ChangeKeyspace(_keyspace);
                _session.Execute(String.Format(TestUtils.CreateTableSimpleFormat, _table));

            _session.Execute(
                new SimpleStatement(
                    String.Format(TestUtils.INSERT_FORMAT, _table, key, "foo", 42, 
                        24.03f.ToString(CultureInfo.InvariantCulture))).SetConsistencyLevel(ConsistencyLevel.All));
            _session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, _table)).SetConsistencyLevel(ConsistencyLevel.All));

            _testCluster.StopForce(2);
            try
            {
                _session.Execute(
                    new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, _table, key, "foo", 42, 
                    24.03f.ToString(CultureInfo.InvariantCulture))).SetConsistencyLevel(ConsistencyLevel.All));
            }
            catch (WriteTimeoutException e)
            {
                Assert.AreEqual(e.ConsistencyLevel, ConsistencyLevel.All);
                Assert.AreEqual(1, e.ReceivedAcknowledgements);
                Assert.AreEqual(2, e.RequiredAcknowledgements);
                Assert.AreEqual(e.WriteType, "SIMPLE");
            }
        }

        [Test]
        public void PreserveStackTraceTest()
        {
            var ex = Assert.Throws<SyntaxError>(() => _session.Execute("SELECT WILL FAIL"));
            StringAssert.Contains(nameof(PreserveStackTraceTest), ex.StackTrace);
            StringAssert.Contains(nameof(ExceptionsTests), ex.StackTrace);
        }

        [Test]
        public void RowSetIteratedTwice()
        {
            var key = "1";

            _session.CreateKeyspaceIfNotExists(_keyspace);
            _session.ChangeKeyspace(_keyspace);
            _session.Execute(String.Format(TestUtils.CreateTableSimpleFormat, _table));
            _session.Execute(new SimpleStatement(string.Format(TestUtils.INSERT_FORMAT, _table, key, "foo", 42, 
                24.03f.ToString(CultureInfo.InvariantCulture))));
            var rowset = _session.Execute(new SimpleStatement(string.Format(TestUtils.SELECT_ALL_FORMAT, _table))).GetRows();
            Assert.NotNull(rowset);
            //Linq Count iterates
            Assert.AreEqual(1, rowset.Count());
            Assert.AreEqual(0, rowset.Count());
        }

        [Test]
        public void RowSetPagingAfterSessionDispose()
        {
            _session.CreateKeyspaceIfNotExists(_keyspace);
            _session.ChangeKeyspace(_keyspace);
            _session.Execute(String.Format(TestUtils.CreateTableSimpleFormat, _table));

            _session.Execute(new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, _table, "1", "foo", 42, 
                24.03f.ToString(CultureInfo.InvariantCulture))));
            _session.Execute(new SimpleStatement(string.Format(TestUtils.INSERT_FORMAT, _table, "2", "foo", 42, 
                24.03f.ToString(CultureInfo.InvariantCulture))));
            var rs = _session.Execute(new SimpleStatement(string.Format(TestUtils.SELECT_ALL_FORMAT, _table)).SetPageSize(1));
            if (CassandraVersion.Major < 2)
            {
                //Paging should be ignored in 1.x
                //But setting the page size should work
                Assert.AreEqual(2, rs.InnerQueueCount);
                return;
            }
            Assert.AreEqual(1, rs.InnerQueueCount);

            _session.Dispose();
            //It should not fail, just do nothing
            rs.FetchMoreResults();
            Assert.AreEqual(1, rs.InnerQueueCount);
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void WriteFailureExceptionTest()
        {
            const string table = KeyspaceNameWriteFailName + ".tbl1";
            using (var cluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build())
            {
                var _session = cluster.Connect();
                _session.Execute(string.Format(TestUtils.CreateKeyspaceSimpleFormat, KeyspaceNameWriteFailName, 2));
                _session.Execute(string.Format(TestUtils.CreateTableSimpleFormat, table));
                var query = string.Format("INSERT INTO {0} (k, t) VALUES ('ONE', 'ONE VALUES')", table);
                Assert.Throws<WriteFailureException>(() => 
                    _session.Execute(new SimpleStatement(query).SetConsistencyLevel(ConsistencyLevel.All)));
            }
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void ReadFailureExceptionTest()
        {
            const string keyspace = "ks_rfail";
            const string table = keyspace + ".tbl1";

            var builder = Cluster
                .Builder()
                .WithLoadBalancingPolicy(new WhiteListLoadBalancingPolicy(2))
                .AddContactPoint(_testCluster.ClusterIpPrefix + "2");
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                session.Execute(string.Format(TestUtils.CreateKeyspaceSimpleFormat, keyspace, 1));
                session.ChangeKeyspace(keyspace);
                session.Execute(string.Format("CREATE TABLE {0} (pk int, cc int, v int, primary key (pk, cc))", table));
                // The rest of the test relies on the fact that the PK '1' will be placed on node1 with MurmurPartitioner
                var ps = session.Prepare(string.Format("INSERT INTO {0} (pk, cc, v) VALUES (1, ?, null)", table));
                var counter = 0;
                TestHelper.ParallelInvoke(() =>
                {
                    var rs = session.Execute(ps.Bind(Interlocked.Increment(ref counter)));
                    Assert.AreEqual(2, TestHelper.GetLastAddressByte(rs.Info.QueriedHost));
                }, 1100);
                Assert.Throws<ReadFailureException>(() => 
                    session.Execute(string.Format("SELECT * FROM {0} WHERE pk = 1", table)));
            }
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void FunctionFailureExceptionTest()
        {
            var builder = Cluster
                .Builder()
                .AddContactPoint(_testCluster.InitialContactPoint);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                session.Execute(string.Format(TestUtils.CreateKeyspaceSimpleFormat, "ks_func", 1));
                session.Execute("CREATE TABLE ks_func.tbl1 (id int PRIMARY KEY, v1 int, v2 int)");
                session.Execute("INSERT INTO ks_func.tbl1 (id, v1, v2) VALUES (1, 1, 0)");
                session.Execute("CREATE FUNCTION ks_func.div(a int, b int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return a / b;'");

                Assert.Throws<FunctionFailureException>(() =>
                    session.Execute("SELECT ks_func.div(v1,v2) FROM ks_func.tbl1 where id = 1"));
            }
        }

        ///////////////////////
        /// Helper Methods
        ///////////////////////

        private class WhiteListLoadBalancingPolicy: ILoadBalancingPolicy
        {
            private readonly ILoadBalancingPolicy _childPolicy = new RoundRobinPolicy();
            private readonly byte[] _list;

            public WhiteListLoadBalancingPolicy(params byte[] listLastOctet)
            {
                _list = listLastOctet;
            }

            public void Initialize(ICluster cluster)
            {
                _childPolicy.Initialize(cluster);
            }

            public HostDistance Distance(Host host)
            {
                return _childPolicy.Distance(host);
            }

            public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
            {   
                var hosts = _childPolicy.NewQueryPlan(keyspace, query);
                return hosts.Where(h => _list.Contains(TestHelper.GetLastAddressByte(h)));
            }
        }
    }
}
