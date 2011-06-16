{-# LANGUAGE GeneralizedNewtypeDeriving, OverloadedStrings,
             ScopedTypeVariables #-}

module Database.Cassandra.Monad
  ( ProtoHandle
  , CassandraConnection(..)
  , CassandraConfig(..)
  , Failure(..)
  , Cassandra
  , CassandraT(..)
  , runCassandraT
  , getConnection
  , getConfig
  , getConsistencyLevel
  , setConsistencyLevel
  , setKeyspace
  , getTime

  {- Re-export -}
  -- Configuration
  , ConsistencyLevel(..)
  , Hostname
  , Keyspace
  , Password
  , Port
  , Username
  -- Error Monad
  , noMsg
  , strMsg
  , throwError
  -- Other
  , Handle
  , MonadIO
  , T.Text
  ) where

import Prelude hiding (catch)

import Control.Exception        ( Handler(..), bracketOnError, catches )
import Control.Monad            ( liftM )
import Control.Monad.Error      ( Error, ErrorT, MonadError, liftIO, noMsg
                                , runErrorT, strMsg, throwError
                                )
import Control.Monad.Reader     ( ReaderT, MonadReader, ask, runReaderT )
import Control.Monad.State      ( StateT, MonadIO, MonadPlus, MonadState
                                , get, put, runStateT
                                )
import Data.Int                 ( Int64 )
import Data.List                ( intercalate )
import Network                  ( PortID(PortNumber) )
import System.IO                ( Handle )
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

import qualified Data.Map  as M
import qualified Data.Text as T
import qualified Database.Cassandra.Thrift.Cassandra_Types as CT

-- | A binary protocol where the handle is wraped in a framed mode.
type ProtoHandle  = BinaryProtocol (FramedTransport Handle)

-- | Carries the connection and handle for the Cassandra connection.
data CassandraConnection = CassandraConnection
    { connection    :: (ProtoHandle, ProtoHandle)
    , handle        :: Handle
    }

-- | Configuration for the Cassandra environment. Operations will be executed
--   accordingly based on these values.
data CassandraConfig = CassandraConfig
    { cassandraKeyspace         ::  Keyspace
    , cassandraConsistencyLevel ::  ConsistencyLevel
    , cassandraHostname         ::  Hostname
    , cassandraPort             ::  Port
    , cassandraUsername         ::  Username
    , cassandraPassword         ::  Password
    }

-- | Represents the possible errors that the Cassandra library can face, either
--   during connection or when executing operations.
data Failure
    = AuthenticationError (Maybe T.Text)
    | AuthorizationError  (Maybe T.Text)
    | InvalidRequest (Maybe T.Text)
    | NotFound
    | SchemaDisagreement
    | TimedOut
    | Unavailable
    | OtherFailure T.Text
    deriving (Eq, Show)

-- | Specializes the CassandraT monad transformer in the IO monad.
type Cassandra a = CassandraT IO a

-- | A monad transformer serving as an environment in which Cassandra commands
--   can be executed.
newtype CassandraT m a = CassandraT
    { unCassandraT :: ReaderT CassandraConnection (StateT CassandraConfig
                        (ErrorT Failure m)) a
    } deriving  ( Functor, Monad, MonadIO, MonadPlus
                , MonadError  Failure
                , MonadReader CassandraConnection
                , MonadState  CassandraConfig
                )

-- | Executes commands within the Cassandra environment when provided with
--   a 'CassandraConnection' (attainable via 'openConnection'). This will close
--   the 'CassandraConnection's handle automatically.
runCassandraT :: MonadIO m => CassandraConfig -> CassandraT m a
              -> m (Either Failure a)
runCassandraT cfg mt = catch' $ do
    conn    <- openConnection cfg
    result  <- runErrorT . (flip runStateT cfg) . (flip runReaderT conn)
             . unCassandraT $ mt
    liftIO . tClose . handle $ conn
    return $ fst `liftM` result

-- | Catches Cassandra-related exceptions and re-represents them as a 'Failure'
--   instead.
catch' :: MonadIO m => m (Either Failure a) -> m (Either Failure a)
catch' input = input >>= \ip ->
    liftIO $ catches (return ip)
        [ Handler $ \(e::CT.AuthenticationException) ->
            retMsg AuthenticationError $ CT.f_AuthenticationException_why e
        , Handler $ \(e::CT.AuthorizationException)  ->
            retMsg AuthorizationError $ CT.f_AuthorizationException_why e
        , Handler $ \(e::CT.InvalidRequestException) ->
            retMsg InvalidRequest $ CT.f_InvalidRequestException_why e
        , Handler $ \(_::CT.SchemaDisagreementException) ->
            ret SchemaDisagreement
        , Handler $ \(_::CT.NotFoundException)    -> ret NotFound
        , Handler $ \(_::CT.TimedOutException)    -> ret TimedOut
        , Handler $ \(_::CT.UnavailableException) -> ret Unavailable
        ]
    where   ret      = return . Left
            retMsg t = ret . t . liftM T.pack

-- | Attempts to open a connection to the Cassandra server based on provided
--   configuration data. Closing the handle is strictly the job of anyone using
--   this function (except in the case of an exception, in which case it will
--   close automatically).
openConnection :: (MonadIO m) => CassandraConfig -> m CassandraConnection
openConnection cfg = liftIO $ bracketOnError (hOpen (host, port)) flush $
    \h -> do
        framed <- openFramedTransport h
        let binpro = BinaryProtocol framed
        let conn   = (binpro, binpro)
        login conn authreq
        set_keyspace conn $ cassandraKeyspace cfg
        return $ CassandraConnection conn h
    where
        host    = cassandraHostname cfg
        port    = PortNumber . fromIntegral . cassandraPort $ cfg
        flush h = tFlush h >> tClose h
        creds   = M.insert "username" (cassandraUsername cfg)
                . M.insert "password" (cassandraPassword cfg)
                $ M.empty
        authreq = AuthenticationRequest
                { f_AuthenticationRequest_credentials = Just creds }

-- | Retrieves 'CassandraConnection' from the 'CassandraT' monad.
getConnection :: (MonadIO m) => CassandraT m (ProtoHandle, ProtoHandle)
getConnection  = connection `liftM` ask

-- | Retrieves 'CassandraConnection' from the 'CassandraT' monad.
getConfig :: (MonadIO m) => CassandraT m CassandraConfig
getConfig  = get

-- | Get the 'ConsistencyLevel' being used in Cassandra operations.
getConsistencyLevel :: (MonadIO m) => CassandraT m ConsistencyLevel
getConsistencyLevel  = cassandraConsistencyLevel `liftM` get

-- | Set the 'ConsistencyLevel' for Cassandra operations.
setConsistencyLevel :: ConsistencyLevel -> Cassandra ()
setConsistencyLevel consistency = do
    config  <- getConfig
    put config { cassandraConsistencyLevel = consistency }
    return ()

-- | Set the keyspace in which Cassandra operations are executed.
setKeyspace :: (MonadIO m) => Keyspace -> CassandraT m ()
setKeyspace keyspace = do
    conn    <- getConnection
    config  <- getConfig
    put config { cassandraKeyspace = keyspace }
    liftIO $ set_keyspace conn keyspace

-- | Cassandra is very sensitive with respect to the timestamp values. As a
--   convention, timestamps are always in microseconds.
getTime :: (MonadIO m) => m Int64
getTime  = liftIO getClockTime >>= \(TOD sec pico) ->
    return . fromInteger $ (sec * 1000000) + (toInteger $ pico `div` 1000000)

instance Error Failure where
    noMsg   = OtherFailure "Unknown error."
    strMsg  = OtherFailure . T.pack

instance Show CassandraConfig where
    show c = intercalate ", "
        [ "KS:"     ++ cassandraKeyspace c
        , "CL:"     ++ (show $ cassandraConsistencyLevel c)
        , "host:"   ++ cassandraHostname c
        , "port:"   ++ (show $ cassandraPort c)
        , "user:"   ++ cassandraUsername c
        , "pass:"   ++ cassandraPassword c
        ]
