ALTER TABLE song_cluster_relationship
ADD COLUMN inferred INTEGER NOT NULL DEFAULT 0
  CHECK (inferred IN (0, 1));
