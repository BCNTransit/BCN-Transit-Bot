from datetime import datetime
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
    username = Column(String, nullable=True)

    email = Column(String, unique=True, index=True, nullable=True)
    firebase_uid = Column(String, unique=True, index=True, nullable=True)
    photo_url = Column(String, nullable=True)
    telegram_id = Column(String, unique=True, index=True, nullable=True)
    source = Column(Enum(UserSource), default=UserSource.ANDROID, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    
    settings = relationship("DBUserSettings", back_populates="user", uselist=False, cascade="all, delete-orphan", lazy="joined")
    
    devices = relationship("UserDevice", back_populates="user", cascade="all, delete-orphan")
    favorites = relationship("Favorite", back_populates="user", cascade="all, delete-orphan")
    audit_trail = relationship("AuditLog", back_populates="user")
    search_history = relationship("DBSearchHistory", back_populates="user")
    user_cards = relationship("DBUserCard", back_populates="user", cascade="all, delete-orphan")


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
    alias = Column(String, nullable=True)

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

    id = Column(String, primary_key=True, index=True) # ej: "bus-175"
    original_id = Column(String, nullable=False, index=True)
    code = Column(String, nullable=False) # "175", "L1"
    name = Column(String, nullable=False, index=True)
    description = Column(String, nullable=True)
    origin = Column(String, nullable=True)
    destination = Column(String, nullable=True)
    color = Column(String, nullable=False)
    transport_type = Column(String, nullable=False)
    
    extra_data = Column(JSON, nullable=True) 

    __table_args__ = (
        UniqueConstraint('original_id', 'transport_type', name='uq_original_id_transport'),
    )
    stops = relationship(
        "DBRouteStop", 
        back_populates="line", 
        order_by="DBRouteStop.order", 
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<DBLine(code={self.code}, name={self.name})>"

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

class DBNotificationLog(Base):
    __tablename__ = "notification_logs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    alert_id = Column(String, nullable=False)
    sent_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_user_alert', 'user_id', 'alert_id'),
    )

class DBUserCard(Base):
    __tablename__ = "user_cards"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    name = Column(String, nullable=False)
    expiration_date = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("DBUser", back_populates="user_cards")

    __table_args__ = (
        Index('idx_user_expiration', 'user_id', 'expiration_date'),
    )

class DBUserSettings(Base):
    __tablename__ = "user_settings"

    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True, index=True)
    
    language = Column(String, default="es", nullable=False)
    theme_mode = Column(String, default="SYSTEM")
    
    general_notifications_enabled = Column(Boolean, default=True, nullable=False) 
    
    card_alerts_enabled = Column(Boolean, default=True)
    card_alert_days_before = Column(Integer, default=3)
    card_alert_hour = Column(Integer, default=9)

    user = relationship("DBUser", back_populates="settings")

class DBPhysicalStation(Base):
    __tablename__ = "physical_stations"

    # ID Normalizado (ej: "237"). Primary Key.
    id = Column(String, primary_key=True, index=True) 
    
    # El código visual (ej: "000237"). Útil para mostrar al usuario.
    code = Column(String, index=True, nullable=True)
    
    name = Column(String, nullable=False)
    description = Column(String, nullable=True) # "Pl. Catalunya / Fontanella"
    transport_type = Column(String, index=True, nullable=False)
    
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    
    # Municipio (Si lo tenías en extra_data, mejor sácalo a columna si filtras por ciudad)
    municipality = Column(String, nullable=True)

    # JSON con servicios extra (Wifi, Accesible, Pantalla led...)
    # Movemos aquí el 'extra_data' antiguo porque suelen ser características del poste.
    extra_data = Column(JSON, nullable=True)

    # Cache de líneas: ["175", "V5", "N12"]
    # Reemplaza a 'connections_data' para pintar el mapa rápido.
    lines_summary = Column(JSON, default=list) 

    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relación inversa: Acceso a todos los RouteStops que ocurren aquí
    route_stops = relationship("DBRouteStop", back_populates="station", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<PhysicalStation(id={self.id}, name={self.name})>"
    
class DBRouteStop(Base):
    __tablename__ = "route_stops"

    # ID autoincremental propio (ya no es un string compuesto raro)
    id = Column(Integer, primary_key=True, autoincrement=True)

    # FK a tu tabla de líneas existente
    # ondelete="CASCADE" asegura que si borras la línea, se borran sus paradas de ruta
    line_id = Column(String, ForeignKey("lines.id", ondelete="CASCADE"), index=True, nullable=False)
    
    # FK a la nueva tabla física
    station_id = Column(String, ForeignKey("physical_stations.id"), index=True, nullable=False)

    order = Column(Integer, nullable=False, index=True)
    
    # Campos opcionales útiles para UI
    direction = Column(String, nullable=True)      # "ida", "vuelta"
    is_origin = Column(Boolean, default=False)
    is_destination = Column(Boolean, default=False)

    # Relaciones
    station = relationship("DBPhysicalStation", back_populates="route_stops")
    
    # Relación con tu tabla de líneas (DBLine)
    # Asumo que tu modelo DBLine existe y tiene tablename="lines"
    line = relationship("DBLine", back_populates="stops") 

    def __repr__(self):
        return f"<RouteStop(line={self.line_id}, station={self.station_id}, order={self.order})>"
    
class DBBicingStation(Base):
    __tablename__ = "bicing_stations"

    id = Column(String, primary_key=True)
    name = Column(String)
    latitude = Column(Float, index=True)
    longitude = Column(Float, index=True)
    
    # Datos dinámicos (se actualizan constantemente)
    slots = Column(Integer)
    mechanical_bikes = Column(Integer)
    electrical_bikes = Column(Integer)
    availability = Column(Integer)
    
    last_updated = Column(DateTime)