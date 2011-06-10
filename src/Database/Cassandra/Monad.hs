{-# LANGUAGE DeriveDataTypeable, GeneralizedNewtypeDeriving #-}
module Database.Cassandra.Monad
  ( withCassandra
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
  , ConsistencyLevel(..)
  , ProtoHandle
  ) where

import Control.Exception        ( bracket )
import Control.Monad.State      ( StateT, get, liftIO, liftM, put, runStateT )
import Data.Int                 ( Int64 )
import Data.List                ( intercalate )
import Data.Map                 ( Map )
import Network                  ( PortID(PortNumber) )
import System.IO                ( hClose, Handle )
import System.Time              ( ClockTime(TOD), getClockTime )
import Thrift.Protocol.Binary   ( BinaryProtocol(..) )
import Thrift.Transport.Handle  ( hOpen )
import Thrift.Transport.Framed  ( FramedTransport, openFramedTransport, tClose
                                , tFlush
                                )

import Database.Cassandra.Types ( Hostname, Keyspace, Password, Port, Username )
import Database.Cassandra.Thrift.Cassandra_Client ( login, set_keyspace )
import Database.Cassandra.Thrift.Cassandra_Types  ( AuthenticationRequest(..)
                                                  , ConsistencyLevel(..)
                                                  )

import qualified Data.Map as M

-- | A non-transformer version of 'CassandraT'.
type Cassandra  a = CassandraT a
-- | A monad encapsulating Cassandra state.
type CassandraT a = StateT CassandraConfig IO a
-- | A binary protocol where the handle is wraped in a framed mode.
type ProtoHandle = BinaryProtocol (FramedTransport Handle)

runCassandraT = runStateT

-- | Configuration for the Cassandra environment. Operations will be executed
--   accordingly based on these values.
data CassandraConfig = CassandraConfig
    { cassandraConnection       ::  (ProtoHandle, ProtoHandle)
    , cassandraKeyspace         ::  String
    , cassandraConsistencyLevel ::  ConsistencyLevel
    , cassandraHostname         ::  Hostname
    , cassandraPort             ::  Port
    , cassandraUsername         ::  Username
    , cassandraPassword         ::  Password
    }

instance Show CassandraConfig where
    show c = intercalate ", "
        [ "KS:"     ++ cassandraKeyspace c
        , "CL:"     ++ (show $ cassandraConsistencyLevel c)
        , "host:"   ++ cassandraHostname c
        , "port:"   ++ (show $ cassandraPort c)
        , "user:"   ++ cassandraUsername c
        , "pass:"   ++ cassandraPassword c
        ]

withCassandra :: CassandraConfig -> Cassandra a -> IO a
withCassandra config callback = bracket
    (hOpen (host, port))
    flushHandle $
    \handle -> do
       framed <- openFramedTransport handle
       let binpro = BinaryProtocol framed
       let conn   = (binpro, binpro)
       let cfg    = cfg { cassandraConnection = conn }
       login        conn (authreq config)
       set_keyspace conn (cassandraKeyspace config)
       fst `liftM` runCassandraT callback cfg
    where
        host          = cassandraHostname config
        port          = PortNumber . fromIntegral . cassandraPort $ config
        flushHandle h = tFlush h >> tClose h
        credmap u p   = M.insert "password" p . M.insert "username" u $ M.empty
        creds cfg     = credmap (cassandraUsername cfg) (cassandraPassword cfg)
        authreq cfg   = AuthenticationRequest
            { f_AuthenticationRequest_credentials = Just . creds $ cfg }

-- | Default configuration for the Cassandra environment. Values can be changed
--   as necessary.
initConfig :: CassandraConfig
initConfig = CassandraConfig
  { cassandraConnection       = undefined
  , cassandraKeyspace         = "system"
  , cassandraConsistencyLevel = ONE
  , cassandraHostname         = "127.0.0.1"
  , cassandraPort             = 9160
  , cassandraUsername         = "default"
  , cassandraPassword         = ""
  }

-- | Get the current Cassandra connection.
getConnection :: Cassandra (ProtoHandle, ProtoHandle)
getConnection  = cassandraConnection `fmap` get -- CassandraT ask

-- | Get the 'ConsistencyLevel' being used in Cassandra operations.
getConsistencyLevel :: Cassandra ConsistencyLevel
getConsistencyLevel = cassandraConsistencyLevel `fmap` get

-- | Set the 'ConsistencyLevel' for Cassandra operations.
setConsistencyLevel :: ConsistencyLevel -> Cassandra ()
setConsistencyLevel consistency =  getCassandra >>=
    \config -> put config { cassandraConsistencyLevel = consistency }

-- | Get the keyspace in which Cassandra operations are being executed.
getKeyspace :: Cassandra Keyspace
getKeyspace = cassandraKeyspace `fmap` get

-- | Set the keyspace in which Cassandra operations are executed.
setKeyspace :: Keyspace -> Cassandra ()
setKeyspace keyspace = do
  config <- getCassandra
  conn   <- getConnection
  liftIO $ set_keyspace conn keyspace
  put config{cassandraKeyspace=keyspace}

-- | Cassandra is very sensitive with respect to the timestamp values. As a
--   convention, timestamps are always in microseconds.
getTime :: Cassandra Int64
getTime = do
  TOD sec pico <- liftIO getClockTime
  return . fromInteger $ (sec * 1000000) + (toInteger $ pico `div` 1000000)

getCassandra :: Cassandra CassandraConfig
getCassandra = get
