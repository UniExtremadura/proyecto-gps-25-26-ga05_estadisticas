CREATE KEYSPACE IF NOT EXISTS estadisticas_keyspace
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE estadisticas_keyspace;

CREATE TABLE IF NOT EXISTS escucha (
  idUsuario int,
  idCancion int,
  fecha timestamp,
  PRIMARY KEY ((idUsuario), fecha)
);

CREATE TABLE IF NOT EXISTS compraAlbum (
  idUsuario int,
  idAlbum int,
  fecha timestamp,
  PRIMARY KEY ((idUsuario), fecha)
);

CREATE TABLE IF NOT EXISTS compraMerch (
  idUsuario int,
  idMerch int,
  fecha timestamp,
  cantidad int,
  PRIMARY KEY ((idUsuario), fecha)
);
