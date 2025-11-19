USE estadisticas_keyspace;

-- Escucha
INSERT INTO escucha (idUsuario, idCancion, fecha) VALUES (1, 1, '2025-01-10T10:00:00');
INSERT INTO escucha (idUsuario, idCancion, fecha) VALUES (1, 2, '2025-02-12T11:30:00');
INSERT INTO escucha (idUsuario, idCancion, fecha) VALUES (2, 3, '2025-03-15T09:20:00');
INSERT INTO escucha (idUsuario, idCancion, fecha) VALUES (3, 4, '2025-04-18T14:45:00');
INSERT INTO escucha (idUsuario, idCancion, fecha) VALUES (4, 5, '2025-05-22T16:10:00');

-- Compra Album
INSERT INTO compraAlbum (idUsuario, idAlbum, fecha) VALUES (1, 1, '2025-01-11T12:00:00');

-- Compra Merch
INSERT INTO compraMerch (idUsuario, idMerch, fecha, cantidad) VALUES (1, 1, '2025-01-12T09:10:00',1);
