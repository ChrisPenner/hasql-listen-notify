-- | @LISTEN@/@NOTIFY@ with @hasql@.
module Hasql.ListenNotify
  ( -- * Listen
    Identifier (..),
    listen,
    unlisten,
    unlistenAll,
    escapeIdentifier,
    Notification (..),
    await,
    poll,
    backendPid,

    -- * Notify
    Notify (..),
    notify,
  )
where

import Control.Monad.Except (ExceptT, runExceptT, throwError)
import Control.Monad.IO.Class
import Data.Functor.Contravariant ((>$<))
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Database.PostgreSQL.LibPQ as LibPQ
import GHC.Conc.IO (threadWaitRead)
import GHC.Generics (Generic)
import qualified Hasql.Decoders as Decoders
import qualified Hasql.Encoders as Encoders
import qualified Hasql.Errors as HasqlErr
import Hasql.Session (Session)
import qualified Hasql.Session as Session
import Hasql.Statement (Statement, preparable, unpreparable)
import System.Posix.Types (CPid)

-- | Listen to a channel.
--
-- https://www.postgresql.org/docs/current/sql-listen.html
listen :: Identifier -> Statement () ()
listen (Identifier chan) =
  unpreparable sql Encoders.noParams Decoders.noResult
  where
    sql :: Text
    sql =
      "LISTEN " <> chan

-- | Stop listening to a channel.
--
-- https://www.postgresql.org/docs/current/sql-unlisten.html
unlisten :: Identifier -> Statement () ()
unlisten (Identifier chan) =
  unpreparable sql Encoders.noParams Decoders.noResult
  where
    sql :: Text
    sql =
      "UNLISTEN " <> chan

-- | Stop listening to all channels.
--
-- https://www.postgresql.org/docs/current/sql-unlisten.html
unlistenAll :: Statement () ()
unlistenAll =
  unpreparable "UNLISTEN *" Encoders.noParams Decoders.noResult

-- | A Postgres identifier.
newtype Identifier
  = Identifier Text
  deriving newtype (Eq, Ord, Show)

-- | Escape a string as a Postgres identifier.
--
--
-- https://www.postgresql.org/docs/15/libpq-exec.html
escapeIdentifier :: Text -> Session Identifier
escapeIdentifier text = do
  libpq (\conn -> runExceptT (escapeIdentifier_ conn text)) >>= \case
    Left err -> throwError err
    Right identifier -> pure (Identifier identifier)

escapeIdentifier_ :: LibPQ.Connection -> Text -> ExceptT HasqlErr.SessionError IO Text
escapeIdentifier_ conn text =
  liftIO (LibPQ.escapeIdentifier conn (Text.encodeUtf8 text)) >>= \case
    Nothing -> throwQueryError conn "PQescapeIdentifier()" [text]
    Just identifier -> pure (Text.decodeUtf8 identifier)

-- | An incoming notification.
data Notification = Notification
  { channel :: !Text,
    payload :: !Text,
    pid :: !CPid
  }
  deriving stock (Eq, Generic, Show)

-- | Get the next notification received from the server.
--
-- https://www.postgresql.org/docs/current/libpq-notify.html
await :: Session Notification
await =
  libpq (\conn -> runExceptT (await_ conn)) >>= \case
    Left err -> throwError err
    Right notification -> pure (parseNotification notification)

await_ :: LibPQ.Connection -> ExceptT HasqlErr.SessionError IO LibPQ.Notify
await_ conn =
  pollForNotification
  where
    pollForNotification :: ExceptT HasqlErr.SessionError IO LibPQ.Notify
    pollForNotification =
      poll_ conn >>= \case
        -- Block until a notification arrives. Snag: the connection might be closed (what). If so, attempt to reset it
        -- and poll for a notification on the new connection.
        Nothing ->
          liftIO (LibPQ.socket conn) >>= \case
            -- "No connection is currently open"
            Nothing -> do
              pqReset conn
              pollForNotification
            Just socket -> do
              liftIO $ threadWaitRead socket
              -- Data has appeared on the socket, but libPQ won't buffer it for us unless we do something (PQexec, etc).
              -- PQconsumeInput is provided for when we don't have anything to do except populate the notification
              -- buffer.
              pqConsumeInput conn
              pollForNotification
        Just notification -> pure notification

-- | Variant of 'await' that doesn't block.
poll :: Session (Maybe Notification)
poll =
  libpq (\conn -> runExceptT (poll_ conn)) >>= \case
    Left err -> throwError err
    Right maybeNotification -> pure (parseNotification <$> maybeNotification)

-- First call `notifies` to pop a notification off of the buffer, if there is one. If there isn't, try `consumeInput` to
-- populate the buffer, followed by another followed by another `notifies`.
poll_ :: LibPQ.Connection -> ExceptT HasqlErr.SessionError IO (Maybe LibPQ.Notify)
poll_ conn =
  liftIO (LibPQ.notifies conn) >>= \case
    Nothing -> do
      pqConsumeInput conn
      liftIO $ LibPQ.notifies conn
    notification -> pure notification

-- | Get the PID of the backend process handling this session. This can be used to filter out notifications that
-- originate from this session.
--
-- https://www.postgresql.org/docs/current/libpq-status.html
backendPid :: Session CPid
backendPid =
  libpq LibPQ.backendPID

-- | An outgoing notification.
data Notify = Notify
  { channel :: !Text,
    payload :: !Text
  }
  deriving stock (Eq, Generic, Show)

-- | Notify a channel.
--
-- https://www.postgresql.org/docs/current/sql-notify.html
notify :: Statement Notify ()
notify =
  preparable sql encoder Decoders.noResult
  where
    sql :: Text
    sql =
      "SELECT pg_notify($1, $2)"

    encoder :: Encoders.Params Notify
    encoder =
      ((\Notify {channel} -> channel) >$< Encoders.param (Encoders.nonNullable Encoders.text))
        <> ((\Notify {payload} -> payload) >$< Encoders.param (Encoders.nonNullable Encoders.text))

------------------------------------------------------------------------------------------------------------------------
-- Little wrappers that throw

pqConsumeInput :: LibPQ.Connection -> ExceptT HasqlErr.SessionError IO ()
pqConsumeInput conn =
  liftIO (LibPQ.consumeInput conn) >>= \case
    False -> throwQueryError conn "PQconsumeInput()" []
    True -> pure ()

pqReset :: LibPQ.Connection -> ExceptT HasqlErr.SessionError IO ()
pqReset conn = do
  liftIO $ LibPQ.reset conn
  liftIO (LibPQ.status conn) >>= \case
    LibPQ.ConnectionOk -> throwQueryError conn "PQreset()" []
    _ -> pure ()

-- Throws a QueryError
throwQueryError :: LibPQ.Connection -> Text -> [Text] -> ExceptT HasqlErr.SessionError IO void
throwQueryError conn context params = do
  message <- fmap Text.decodeUtf8 <$> liftIO (LibPQ.errorMessage conn)
  throwError (HasqlErr.DriverSessionError ("hasql-listen-notify:error:" <> context <> ": " <> Text.pack (show params) <> (maybe "" (" : " <>) message)))

libpq :: (LibPQ.Connection -> IO a) -> Session a
libpq action = do
  Session.onLibpqConnection \libpqConn -> do
    a <- liftIO $ action libpqConn
    pure $ (Right a, libpqConn)

-- Parse a Notify from a LibPQ.Notify
parseNotification :: LibPQ.Notify -> Notification
parseNotification notification =
  Notification
    { channel = Text.decodeUtf8 (LibPQ.notifyRelname notification),
      payload = Text.decodeUtf8 (LibPQ.notifyExtra notification),
      pid = LibPQ.notifyBePid notification
    }
