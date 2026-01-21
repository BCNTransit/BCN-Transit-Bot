from enum import Enum as PyEnum
from sqlalchemy import JSON, Column, Index, Integer, String, DateTime, ForeignKey, Boolean, Float, BigInteger, UniqueConstraint, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB
from src.infrastructure.database.base import Base 

# Definimos el Enum para usarlo en la columna
class UserSource(str, PyEnum):
    ANDROID = "android"
    TELEGRAM = "telegram"

# ----------------------------
# USUARIOS
# ----------------------------
class DBUser(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)    
    
    # GOOGLE
    email = Column(String, unique=True, index=True, nullable=True)
    firebase_uid = Column(String, unique=True, index=True, nullable=True)
    photo_url = Column(String, nullable=True)
    
    # TELEGRAM
    telegram_id = Column(String, unique=True, index=True, nullable=True)
    
    # METADATOS
    # MEJORA: Usar Enum de SQLAlchemy para validar datos a nivel de BD/ORM
    source = Column(Enum(UserSource), default=UserSource.ANDROID, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    
    language = Column(String, default="es")
    
    # --- RELACIONES ---
    # Aquí faltaban las definiciones para que funcionen los back_populates de las otras tablas
    devices = relationship("UserDevice", back_populates="user", cascade="all, delete-orphan")
    favorites = relationship("Favorite", back_populates="user", cascade="all, delete-orphan")
    audit_trail = relationship("AuditLog", back_populates="user")
    search_history = relationship("DBSearchHistory", back_populates="user")


class UserDevice(Base):
    __tablename__ = "user_devices"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # TU LLAVE MAESTRA
    installation_id = Column(String, index=True, nullable=False) 
    
    # TOKEN NOTIFICACIONES
    fcm_token = Column(String, nullable=False)
    
    last_active = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Usamos string "DBUser" para evitar problemas de importación circular
    user = relationship("DBUser", back_populates="devices")

# ----------------------------
# FAVORITOS
# ----------------------------
class Favorite(Base):
    __tablename__ = "favorites"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    transport_type = Column(String, nullable=False)
    station_code = Column(String, nullable=False)
    station_name = Column(String, nullable=False)
    station_group_code = Column(String, nullable=True)
    
    line_name = Column(String, nullable=True)
    line_name_with_emoji = Column(String, nullable=True)
    line_code = Column(String, nullable=True)    

    latitude = Column(Float, nullable=True) 
    longitude = Column(Float, nullable=True)

    user = relationship("DBUser", back_populates="favorites")

# ----------------------------
# DATOS DE SERVICIO (TMB/RODALIES)
# ----------------------------
class Alert(Base):
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True)
    external_id = Column(String, unique=True)
    
    transport_type = Column(String)
    begin_date = Column(DateTime)
    end_date = Column(DateTime, nullable=True)
    
    status = Column(String)
    cause = Column(String)
    
    publications = Column(JSONB) 
    affected_entities = Column(JSONB)

# ----------------------------
# AUDIT & HISTORY
# ----------------------------
class AuditLog(Base):
    __tablename__ = "audit_trail"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, server_default=func.now())
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    client_source = Column(String, index=True, nullable=False)
    
    action = Column(String)
    details = Column(JSONB)
    
    user = relationship("DBUser", back_populates="audit_trail")
    
    __table_args__ = (
        Index('ix_audit_details', details, postgresql_using='gin'),
    )

class DBSearchHistory(Base):
    __tablename__ = "search_history"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    query = Column(String)
    timestamp = Column(DateTime, server_default=func.now())
    
    user = relationship("DBUser", back_populates="search_history")


# ----------------------------
# LINES
# ----------------------------
class DBLine(Base):
    __tablename__ = "lines"

    id = Column(String, primary_key=True, index=True)
    original_id = Column(String, nullable=False, index=True)
    code = Column(String, nullable=False)
    name = Column(String, nullable=False, index=True)
    description = Column(String, nullable=True)
    origin = Column(String, nullable=True)
    destination = Column(String, nullable=True)
    color = Column(String, nullable=False)
    transport_type = Column(String, nullable=False)
    extra_data = Column(JSON, nullable=True) # JSON normal porque rara vez consultamos dentro

    __table_args__ = (
        UniqueConstraint('original_id', 'transport_type', name='uq_original_id_transport'),
    )

# ----------------------------
# STATIONS
# ----------------------------
class DBStation(Base):
    __tablename__ = "stations"

    id = Column(String, primary_key=True, index=True)    
    original_id = Column(String, index=True)
    
    code = Column(String, index=True)
    name = Column(String)
    description = Column(String, nullable=True)
    
    latitude = Column(Float)
    longitude = Column(Float)
    order = Column(Integer)
    
    transport_type = Column(String, index=True)

    line_id = Column(String, ForeignKey("lines.id", ondelete="CASCADE"), index=True)
    
    # Backref crea la relación inversa en DBLine automáticamente como 'stations_rel'
    line = relationship("DBLine", backref="stations_rel") 

    connections_data = Column(JSON, nullable=True) 
    extra_data = Column(JSON, nullable=True)