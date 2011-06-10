{-# LANGUAGE RankNTypes #-}

module Database.Cassandra
    ( Column(..)
    , (=:)
    , (=|)
    , insert
    , remove
    , Filter(..)
    , get
    , multiget
    , range
    , columns

    , withCassandra
    , CassandraConfig(..)
    , initConfig
    , getConnection
    , getKeyspace
    , setKeyspace
    , getConsistencyLevel
    , setConsistencyLevel
    , getTime
    , getCassandra
    , Cassandra
    , CassandraT
    ) where

import Control.Monad.Trans      ( liftIO )
import Data.ByteString.Lazy     ( ByteString )
import Data.Int                 ( Int32 )
import Data.Map                 ( Map )
import Database.Cassandra.Monad ( Cassandra, CassandraConfig, CassandraT
                                , getKeyspace, getCassandra, getConnection
                                , getConsistencyLevel, getTime, initConfig
                                , setKeyspace, setConsistencyLevel
                                , withCassandra
                                )
import Database.Cassandra.Types ( BS, bs, ColumnFamily, ColumnName
                                , ColumnValue
                                )

import qualified Data.ByteString.Lazy.Char8 as L
import qualified Database.Cassandra.Thrift.Cassandra_Types  as T
import qualified Database.Cassandra.Thrift.Cassandra_Client as C
import qualified Data.Map as M

data Column = Column ColumnName ColumnValue
            | Super  ColumnName [Column]
            deriving (Show)

-- Build up a column's insert values
(=:) :: (BS column, BS value) => column -> value -> Column
(=:) col val = Column (bs col) (bs val)

-- Build up a super column's insert values
(=|) :: (BS supercolumn) => supercolumn -> [Column] -> Column
(=|) sup  = Super (bs sup)

-- insert "Users" "necrobious@gmail.com"
--   [ "fn"      =: "Kirk"
--   , "ln"      =: "Peterson"
--   , "Address" =| [ "street1" =: "2020"
--                  , "state"   =: "Oregon"
--                  ]
--   ]
insert :: (BS key) => ColumnFamily -> key -> [Column] -> Cassandra ()
insert column_family key columns = do
    consistency   <- getConsistencyLevel
    conn          <- getConnection
    now           <- getTime
    let mtMap = M.singleton (bs key) $ M.singleton column_family (mutations now)
    liftIO $ C.batch_mutate conn mtMap consistency
    where
        mutations now       = map (q now) columns
        colOrSuperCol c sc  = T.ColumnOrSuperColumn c sc Nothing Nothing
        column c            = colOrSuperCol (Just c) Nothing
        superCol sc         = colOrSuperCol Nothing (Just sc)
        col c v now         = T.Column (Just c) (Just v) (Just now) Nothing
        m now (Column c v)  = col c v now
        q now c             = T.Mutation (Just $ f now c) Nothing
        f now (Column c v)  = column $ col c v now
        f now (Super s cs)  = superCol $ T.SuperColumn (Just s)
                                         (Just $ map (m now) cs)

-- | Given a particular column family and key, removes either a SuperColumn or
--   particular (named) columns (belonging to either a column family or super
--   column) based on the filter. E.g.,
--
-- > remove "Users" "necrobious@gmail.com" (columns ["fn", "address" , "ln"])
remove :: (BS key) => ColumnFamily -> key -> Filter  -> Cassandra ()
remove column_family key fltr = do
    consistency <- getConsistencyLevel
    conn        <- getConnection
    now         <- getTime
    let mts = mtMap . mutations now Nothing
    case fltr of
        AllColumns     -> liftIO $ C.remove conn key' colPath now consistency
        ColNames []    -> liftIO $ C.remove conn key' colPath now consistency
        ColNames cs    -> liftIO $ C.batch_mutate conn (mts cs) consistency
        SupNames sc cs -> liftIO $ C.batch_mutate conn (mts cs) consistency
        _              -> return ()
    where
        key'    = bs key
        colPath = T.ColumnPath (Just column_family) Nothing Nothing
        mtMap   = M.singleton (bs key) . M.singleton column_family
        mutations now sup columns = [m now sup columns]
        m now sup cs = T.Mutation Nothing (Just $ delete now sup cs)

-- | Deletes either the entire SuperColumn or particular (named) columns
--   (either standalone or as members of a SuperColumn).
delete now sup@(Just _) [] = deletion now sup Nothing
delete now sup@(Just _) cs = deletion now sup (Just . slicePredicate $ cs)
delete now Nothing      [] = deletion now Nothing Nothing
delete now Nothing      cs = deletion now Nothing (Just . slicePredicate $ cs)

-- | Convenience function to create a deletion with a provided time.
deletion = T.Deletion . Just

-- | Creates a SlicePredicate based on an input list of columns.
slicePredicate cs = T.SlicePredicate (Just cs) Nothing

data Filter =
      AllColumns
    | ColNames [ByteString]
    | SupNames ByteString [ByteString]
    | ColRange
        { rangeStart   :: ByteString
        , rangeEnd     :: ByteString
        , rangeReverse :: Bool
        , rangeLimit   :: Int32
        }

-- | a smarter constructor for building a Range filter
range :: forall column_name. (BS column_name) => column_name -> column_name
      -> Bool -> Int32 -> Filter
range start finish = ColRange (bs start) (bs finish)

-- | a smarter constructor for building a Columns filter
columns :: forall column_name. (BS column_name) => [column_name] -> Filter
columns = ColNames . (map bs)

supercolumns :: forall column_name. (BS column_name) => column_name
             -> [column_name] -> Filter
supercolumns sc cs = SupNames (bs sc) (map bs cs)

-- | Retrieve all columns (or those allowed by the filter) for a given key
--   within a given Column Family.
get :: (BS key) => ColumnFamily -> key -> Filter -> Cassandra [Column]
get cf key fltr = do
    consistency <- getConsistencyLevel
    conn    <- getConnection
    results <- liftIO $ C.get_slice conn key' cp sp consistency
    return   $ foldr rewrap [] results
    where key'  = bs key
          cp    = column_parent cf fltr
          sp    = slice_predicate fltr

get_count :: (BS key) => ColumnFamily -> key -> Filter -> Cassandra Int32
get_count cf key fltr = do
    consistency <- getConsistencyLevel
    conn        <- getConnection
    liftIO $ C.get_count conn key' cp sp consistency
    where key'  = bs key
          cp    = column_parent cf fltr
          sp    = slice_predicate fltr

multiget :: (BS key) => ColumnFamily -> [key] -> Filter
         -> Cassandra (Map key [Column])
multiget cf keys fltr = do
    let byBs     = keys2map keys
    consistency <- getConsistencyLevel
    conn        <- getConnection
    let mKeys    = M.keys byBs
    results     <- liftIO $ C.multiget_slice conn mKeys cp sp consistency
    return . map2map byBs $ M.foldrWithKey remap M.empty results
    where cp = column_parent cf fltr
          sp = slice_predicate fltr

keys2map :: (BS key) => [key] -> Map ByteString key
keys2map keys = foldr (\ k m -> M.insert (bs k) k m)  M.empty keys

map2map :: (BS key) => Map ByteString key ->  Map ByteString a -> Map key a
map2map lookupMap resultsMap = M.foldrWithKey foldOver M.empty resultsMap
    where foldOver resultKey val accMap =
            case M.lookup resultKey lookupMap of
                Just lookupKey -> M.insert lookupKey val accMap
                Nothing        -> accMap


remap :: (BS key) => key -> [T.ColumnOrSuperColumn] -> Map key [Column]
      -> Map key [Column]
remap key cols acc = M.insert key (foldr rewrap [] cols) acc

rewrap :: T.ColumnOrSuperColumn -> [Column] -> [Column]
rewrap  (T.ColumnOrSuperColumn
              (Just (T.Column (Just n) (Just v) _ _))
              Nothing
              Nothing
              Nothing
        ) acc  = (Column n v) : acc
rewrap  (T.ColumnOrSuperColumn
              Nothing
              (Just (T.SuperColumn (Just n) (Just cs)))
              Nothing
              Nothing
        ) acc = (Super n (foldr c2c [] cs)) : acc
rewrap _ acc  = acc

c2c :: T.Column -> [Column] -> [Column]
c2c (T.Column  (Just n) (Just v) _ _) acc = (Column n v) : acc
c2c _ acc = acc

column_parent :: ColumnFamily -> Filter -> T.ColumnParent
column_parent cf (SupNames sc _) = T.ColumnParent (Just cf) (Just sc)
column_parent cf _               = T.ColumnParent (Just cf) Nothing

slice_predicate :: Filter -> T.SlicePredicate
slice_predicate (ColNames bs)           = T.SlicePredicate (Just bs) Nothing
slice_predicate (SupNames _ bs)         = T.SlicePredicate (Just bs) Nothing
slice_predicate (ColRange rs re rr rl)  = T.SlicePredicate Nothing (Just range)
    where range = T.SliceRange (Just rs) (Just re) (Just rr) (Just rl)
slice_predicate AllColumns              = T.SlicePredicate Nothing (Just range)
    where range = T.SliceRange  (Just L.empty) (Just L.empty) (Just False)
                                (Just 100)
