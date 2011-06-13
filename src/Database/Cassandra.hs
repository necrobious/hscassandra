{-# LANGUAGE RankNTypes #-}

module Database.Cassandra
    ( -- $doc
      Column(..)
    , Filter(..)
    , (=:)
    , (=|)
    , get
    , getCount
    , insert
    , multiget
    , remove

    {- Re-Export -}
    -- Cassandra Monad
    , Cassandra
    , CassandraT
    , CassandraConfig(..)
    , Failure(..)
    , runCassandraT
    , noMsg
    , strMsg
    , throwError
    -- Configuration
    , ConsistencyLevel(..)
    , Hostname
    , Keyspace
    , Password
    , Port
    , Username
    -- Cassandra Types
    , ColumnFamily
    , ColumnName
    , ColumnValue
    , Key
    , SuperColumnName
    -- Other
    , M.Map
    , Text
    ) where

import Control.Monad.Trans      ( liftIO )
import Data.ByteString.Lazy     ( ByteString )
import Data.Int                 ( Int32, Int64 )
import Data.Map                 ( Map )
import Data.Maybe               ( fromJust )
import Database.Cassandra.Monad ( Cassandra, CassandraT, CassandraConfig(..)
                                , ConsistencyLevel(..) , Failure(..), Hostname
                                , Keyspace, MonadIO, Password, Port, Text
                                , Username, getConnection , getConsistencyLevel
                                , getTime, noMsg, runCassandraT, strMsg
                                , throwError
                                )
import Database.Cassandra.Types ( ColumnFamily, ColumnName
                                , ColumnValue, Key, SuperColumnName
                                )

import qualified Data.ByteString.Lazy.Char8 as L
import qualified Database.Cassandra.Thrift.Cassandra_Types  as T
import qualified Database.Cassandra.Thrift.Cassandra_Client as C
import qualified Data.Map as M

-- | Represents either a column, identified by its name and an accompanying
--   value, or a super column, identified by its name and a list of sub-columns.
data Column = Column ColumnName ColumnValue
            | Super  ColumnName [Column]
            deriving (Show)

-- | Filter operation based on particular column parameters.
data Filter =
       -- | Matches all columns
      AllColumns
       -- | Matches columns specified by name.
    | ColNames [ByteString]
      -- | Matches a particular super column and its sub-columns (identified by
      --   name).
    | SupNames ByteString [ByteString]
      -- | Cassandra's LIMIT and ORDER BY equivalents. See
      --   <http://wiki.apache.org/cassandra/API#SliceRange>
    | ColRange  { rangeStart   :: ByteString
                , rangeEnd     :: ByteString
                , rangeReverse :: Bool
                , rangeLimit   :: Int32
                }

-- | Build up a column's insert values
(=:) :: ColumnName -> ColumnValue -> Column
(=:) col val = Column col val

-- | Build up a super column's insert values
(=|) :: SuperColumnName -> [Column] -> Column
(=|) sup = Super sup

-- | Retrieve all columns (or those allowed by the filter) for a given key
--   within a given Column Family.
--
--   See <http://wiki.apache.org/cassandra/API#get> for more info.
get :: (MonadIO m) => ColumnFamily -> Key -> Filter -> CassandraT m [Column]
get cf key fltr = do
    consistency <- getConsistencyLevel
    conn    <- getConnection
    results <- liftIO $ C.get_slice conn key cp sp consistency
    return   $ foldr rewrap [] results
    where cp    = column_parent cf fltr
          sp    = slice_predicate fltr

-- | Counts the elements present in a column, identified by key, within
--   a specified ColumnFamily, matching the filter.
--
--   Please note that this method is not @O(1)@ as it must take all columns
--   from disk to calculate the answer. There is, however, a benefit that they
--   do not have to be pulled to the client for counting.
--
--   See <http://wiki.apache.org/cassandra/API#get_count> for more info.
getCount :: (MonadIO m) => ColumnFamily -> Key -> Filter -> CassandraT m Int32
getCount cf key fltr = do
    consistency <- getConsistencyLevel
    conn        <- getConnection
    liftIO $ C.get_count conn key cp sp consistency
    where cp    = column_parent cf fltr
          sp    = slice_predicate fltr

-- | Given a particular column family and key, inserts columns consisting of
--   name-value pairs. E.g.,
--
-- > insert "Users" "necrobious@gmail.com"
-- >     [ "fn"      =: "Kirk"
-- >     , "ln"      =: "Peterson"
-- >     , "Address" =| [ "street1" =: "2020"
-- >                    , "state"   =: "Oregon"
-- >                    ]
-- >     ]
insert :: (MonadIO m) => ColumnFamily -> Key -> [Column] -> CassandraT m ()
insert column_family key cols = do
    consistency   <- getConsistencyLevel
    conn          <- getConnection
    now           <- getTime
    let mtMap = M.singleton key $ M.singleton column_family (mutations now)
    liftIO $ C.batch_mutate conn mtMap consistency
    where
        mutations now       = map (q now) cols
        colOrSuperCol c sc  = T.ColumnOrSuperColumn c sc Nothing Nothing
        column c            = colOrSuperCol (Just c) Nothing
        superCol sc         = colOrSuperCol Nothing (Just sc)
        col c v now         = T.Column (Just c) (Just v) (Just now) Nothing
        m now (Column c v)  = col c v now
        q now c             = T.Mutation (Just $ f now c) Nothing
        f now (Column c v)  = column $ col c v now
        f now (Super s cs)  = superCol $ T.SuperColumn (Just s)
                                         (Just $ map (m now) cs)

-- | For a specified Column Family, retrieves all columns matching the filter
--   and identified by one of the supplied keys.
--
--   See <http://wiki.apache.org/cassandra/API#multiget_slice> for more info.
multiget :: (MonadIO m) => ColumnFamily -> [Key] -> Filter
         -> CassandraT m (Map Key [Column])
multiget cf keys fltr = do
    let byBs     = keys2map keys
    consistency <- getConsistencyLevel
    conn        <- getConnection
    let mKeys    = M.keys byBs
    results     <- liftIO $ C.multiget_slice conn mKeys cp sp consistency
    return . map2map byBs $ M.foldrWithKey remap M.empty results
    where cp = column_parent cf fltr
          sp = slice_predicate fltr

-- | Given a particular column family and key, removes either a SuperColumn or
--   particular (named) columns (belonging to either a column family or super
--   column) based on the filter. E.g.,
--
-- > remove "Users" "necrobious@gmail.com" (ColNames ["fn", "address" , "ln"])
remove :: (MonadIO m) => ColumnFamily -> Key -> Filter  -> CassandraT m ()
remove column_family key fltr = do
    consistency <- getConsistencyLevel
    conn        <- getConnection
    now         <- getTime
    let mts s c = mtMap $ mutations now s c
    let bMutate = C.batch_mutate conn
    case fltr of
        AllColumns     -> liftIO $ C.remove conn key colPath now consistency
        ColNames []    -> liftIO $ C.remove conn key colPath now consistency
        ColNames cs    -> liftIO $ bMutate (mts Nothing cs) consistency
        SupNames sc cs -> liftIO $ bMutate (mts (Just sc) cs) consistency
        _              -> return ()
    where
        colPath = T.ColumnPath (Just column_family) Nothing Nothing
        mtMap   = M.singleton key . M.singleton column_family
        mutations now sup cs = [m now sup cs]
        m now sup cs = T.Mutation Nothing (Just $ delete now sup cs)

-- Deletes either the entire SuperColumn or particular (named) columns
-- (either standalone or as members of a SuperColumn).
delete :: Int64 -> Maybe ByteString -> [ByteString] -> T.Deletion
delete now sup@(Just _) [] = deletion now sup Nothing
delete now sup@(Just _) cs = deletion now sup (Just . slicePredicate $ cs)
delete now Nothing      [] = deletion now Nothing Nothing
delete now Nothing      cs = deletion now Nothing (Just . slicePredicate $ cs)

-- Convenience function to create a deletion with a provided time.
deletion :: Int64 -> Maybe ByteString -> Maybe T.SlicePredicate -> T.Deletion
deletion  = T.Deletion . Just

-- Creates a SlicePredicate based on an input list of columns.
slicePredicate :: [ColumnName] -> T.SlicePredicate
slicePredicate cs = T.SlicePredicate (Just cs) Nothing

-- Turns a list of keys into a map.
keys2map :: [Key] -> Map ByteString Key
keys2map keys = foldr (\k m -> M.insert k k m) M.empty keys

-- Joins two maps based on their key, producing a new map which uses the value
-- of the first map as a key and the value of the second map as a value.
map2map :: Map ByteString Key ->  Map ByteString a -> Map Key a
map2map lookupMap resultsMap = M.foldrWithKey foldOver M.empty resultsMap
    where foldOver resultKey val accMap =
            case M.lookup resultKey lookupMap of
                Just lookupKey -> M.insert lookupKey val accMap
                Nothing        -> accMap

-- | Joins a list of columns or super columns with an existing map of keys and
--   columns.
remap :: Key -> [T.ColumnOrSuperColumn] -> Map Key [Column] -> Map Key [Column]
remap key cols acc = M.insert key (foldr rewrap [] cols) acc

-- | Joins a list of columns or super columns with an existing list of columns.
rewrap :: T.ColumnOrSuperColumn -> [Column] -> [Column]
rewrap (T.ColumnOrSuperColumn (Just col) Nothing Nothing Nothing) acc =
    let name = fromJust $ T.f_Column_name  col
        val  = fromJust $ T.f_Column_value col
    in (Column name val) : acc
rewrap (T.ColumnOrSuperColumn Nothing (Just sc) Nothing Nothing) acc =
    let name = fromJust $ T.f_SuperColumn_name sc
        cols = fromJust $ T.f_SuperColumn_columns sc
    in (Super name (foldr c2c [] cols)) : acc
rewrap _ acc  = acc

-- Joins a Cassandra-Thrift 'T.Column' with a list of Columns, turning it
-- into a 'Column' in the process.
c2c :: T.Column -> [Column] -> [Column]
c2c (T.Column (Just n) (Just v) _ _) acc = (Column n v) : acc
c2c _ acc = acc

-- Uses a specified 'ColumnFamily' and 'Filter' to produce a 'T.ColumnParent',
-- which is understood by Cassandra as the path to a particular set of columns.
-- See <http://wiki.apache.org/cassandra/API#ColumnParent>.
column_parent :: ColumnFamily -> Filter -> T.ColumnParent
column_parent cf (SupNames sc _) = T.ColumnParent (Just cf) (Just sc)
column_parent cf _               = T.ColumnParent (Just cf) Nothing

-- Turns a 'Filter' into a 'T.SlicePredicate', which is understood by Cassandra
-- for selecting columns in various operations. See
-- <http://wiki.apache.org/cassandra/API#SlicePredicate>.
slice_predicate :: Filter -> T.SlicePredicate
slice_predicate (ColNames ns)           = T.SlicePredicate (Just ns) Nothing
slice_predicate (SupNames _ ns)         = T.SlicePredicate (Just ns) Nothing
slice_predicate (ColRange rs re rr rl)  = T.SlicePredicate Nothing (Just range)
    where range = T.SliceRange (Just rs) (Just re) (Just rr) (Just rl)
slice_predicate AllColumns              = T.SlicePredicate Nothing (Just range)
    where range = T.SliceRange (Just L.empty) (Just L.empty) (Just False)
                               (Just 100)

-- $doc
--
-- This module is a wrapper around the cassandra-thrift
-- (<http://hackage.haskell.org/package/cassandra-thrift>) package, which
-- implements the Apache Cassandra API via Thrift.
--
-- Here, operations are done through the 'CassandraT' monad, which encapsulates
-- the handling of the configuration while also catching common errors, making
-- Cassandra easy to use from a client's perspective. Some basic usage follows.
--
-- The below examples assume the @NoMonomorphismRestriction@ and
-- @OverloadedStrings@ LANGUAGE pragmas.
--
-- First, we must define our connection to Cassandra by using 'CassandraConfig'.
--
-- > config = CassandraConfig   { cassandraKeyspace          = "MyKeyspace"
-- >                             , cassandraConsistencyLevel = ONE
-- >                             , cassandraHostname         = "127.0.0.1"
-- >                             , cassandraPort             = 9160
-- >                             , cassandraUsername         = ""
-- >                             , cassandraPassword         = ""
-- >                             }
--
-- We can then run the Cassandra monad and begin executing our operations:
--
-- > works :: MonadIO m => m (Either Failure [Column])
-- > works  = runCassandraT config $ do
-- >     insert "Users" "michael" [ "name"      =: "Michael Schade"
-- >                              , "email"     =: "hackage@spearheaddev.com"
-- >                              , "website"   =: "http://spearheaddev.com/"
-- >                              ]
-- >     get "Users" "michael" AllColumns
--
-- This would return:
--
-- > Right [Column (Chunk "email" Empty) (Chunk "hackage@mschade.me" Empty), ...
--
-- As mentioned, error handling is incorporated into the 'CassandraT' monad. If
-- we tried code similar to above, but using the nonexistent (and so
-- conveniently named) `NonExistentCF` column family, we helpfully be returned
-- a @m (Left Failure)@. For example,
--
-- > fails :: MonadIO m => m (Either Failure [Column])
-- > fails  = runCassandraT config $ do
-- >    get "NonExistentCF" "michael" AllColumns
--
-- returns
--
-- > Left (InvalidRequest (Just "unconfigured columnfamily NonExistentCF"))
