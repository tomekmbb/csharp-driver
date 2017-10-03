using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqTable
{
    [TestFixture, Category("short")]
    public class LinqUdtTests : SharedClusterTest
    {
        ISession _session = null;
        private readonly string _uniqueKeyspaceName = TestUtils.GetUniqueKeyspaceName();
        private readonly Guid _sampleId = Guid.NewGuid();

        public override void OneTimeSetUp()
        {
            if (CassandraVersion < Version.Parse("2.1.0"))
                Assert.Ignore("Requires Cassandra version >= 2.1");
            
            base.OneTimeSetUp();
            _session = Session;

            _session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, _uniqueKeyspaceName, 1));
            _session.ChangeKeyspace(_uniqueKeyspaceName);
            _session.Execute("CREATE TYPE song (id uuid, title text, artist text)");
            _session.Execute("CREATE TYPE album (id uuid, name text, publishingdate timestamp, songs list<frozen<song>>)");
            _session.UserDefinedTypes.Define(UdtMap.For<Song>());
            _session.UserDefinedTypes.Define(UdtMap.For<Album>());
        }

        [Test, TestCassandraVersion(2, 1, 0)]
        public void LinqUdt_Select()
        {
            // Avoid interfering with other tests
            _session.Execute("DROP TABLE IF EXISTS band");
            _session.Execute("CREATE TABLE band (id uuid primary key, name text, albuns list<frozen<album>>)");
            _session.Execute(
                new SimpleStatement(
                    "INSERT INTO band (id, name, albuns) VALUES (?, 'Ben Folds Five', [" +
                    "{id: uuid(), name:'Whatever and Ever Amen', publishingdate: '1997-02-05 04:05:00+0000', songs: [{id: uuid(), title: 'Kate', artist: 'Ben Folds Five'}," +
                    "{id: uuid(), title: 'Brick', artist: 'Ben Folds Five'}]}])",
                    _sampleId));

            var table = new Table<Band>(_session, new MappingConfiguration().Define(new Map<Band>().TableName("band")));
            var band = table.Select(a => new Band { Id = a.Id, Name = a.Name, Albuns = a.Albuns }).Execute().First();
            Assert.AreEqual(_sampleId, band.Id);
            Assert.AreEqual("Ben Folds Five", band.Name);
            Assert.NotNull(band.Albuns);
            Assert.AreEqual(1, band.Albuns.Count);
            Assert.AreEqual(2, band.Albuns[0].Songs.Count);
            Assert.NotNull(band.Albuns[0].PublishingDate);
            var song1 =  band.Albuns[0].Songs[0];
            var song2 =  band.Albuns[0].Songs[1];
            Assert.AreEqual("Kate", song1.Title);
            Assert.AreEqual("Ben Folds Five", song1.Artist);
            Assert.AreEqual("Brick", song2.Title);
            Assert.AreEqual("Ben Folds Five", song2.Artist);
        }

        [Test, TestCassandraVersion(2,1,0)]
        public void LinqUdt_Insert()
        {
            // Avoid interfering with other tests
            _session.Execute("DROP TABLE IF EXISTS band");
            _session.Execute("CREATE TABLE band (id uuid primary key, name text, albuns list<frozen<album>>)");

            var table = new Table<Band>(_session, new MappingConfiguration().Define(new Map<Band>().TableName("band")));
            var id = Guid.NewGuid();
            var band = new Band
            {
                Id = id,
                Name = "Foo Fighters",
                Albuns = new List<Album>
                {
                    new Album
                    {
                        Id = Guid.NewGuid(),
                        Name = "The Colour and the Shape",
                        PublishingDate = DateTimeOffset.Parse("1997-05-20"),
                        Songs = new List<Song>
                        {
                            new Song
                            {
                                Id = Guid.NewGuid(),
                                Artist = "Foo Fighters",
                                Title = "Monkey Wrench"
                            },
                            new Song
                            {
                                Id = Guid.NewGuid(),
                                Artist = "Foo Fighters",
                                Title = "Everlong"
                            }
                        }
                    }
                }
            };
            table.Insert(band).Execute();
            //Check that the values exists using core driver
            var row = _session.Execute(new SimpleStatement("SELECT * FROM band WHERE id = ?", id)).First();
            Assert.AreEqual("Foo Fighters", row.GetValue<object>("name"));
            var albuns = row.GetValue<List<Album>>("albuns");
            Assert.NotNull(albuns);
            Assert.AreEqual(1, albuns.Count);
            var songs = albuns[0].Songs;
            Assert.NotNull(songs);
            Assert.AreEqual(2, songs.Count);
            Assert.NotNull(songs.FirstOrDefault(s => s.Title == "Monkey Wrench"));
            Assert.NotNull(songs.FirstOrDefault(s => s.Title == "Everlong"));
        }

//        [Test, TestCassandraVersion(2, 1, 0)]
//        public void LinqUdt_Select_Enum()
//        {
//            // Avoid interfering with other tests
//            _session.Execute("DROP TABLE IF EXISTS someClass");
//            _session.Execute("CREATE TABLE someClass (id uuid primary key, name text, someUdt list<frozen<someUdt>>)");
//            _session.Execute(
//                new SimpleStatement(
//                    "INSERT INTO someClass (id, name, someUdt) VALUES (?, 'Legend', [{someEnum: 'ONE'}])",
//                    _sampleId));
//
//            var table = new Table<SomeClass>(_session, new MappingConfiguration().Define(new Map<SomeClass>().TableName("someClass")));
//            var album = table.Select(a => new SomeClass { Id = a.Id, Name = a.Name, SomeUdt = a.SomeUdt}).Execute().First();
//            Assert.AreEqual(_sampleId, album.Id);
//            Assert.AreEqual("Legend", album.Name);
//            Assert.NotNull(album.SomeUdt);
//            Assert.AreEqual(1, album.SomeUdt.Count);
//            var someUdt = album.SomeUdt[0];
//            Assert.AreEqual(SomeEnum.ONE, someUdt.SomeEnum);
//        }
        
    }
}
