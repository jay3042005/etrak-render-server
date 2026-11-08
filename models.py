"""
Pydantic models for request validation in eTrakYan API
"""

from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional, List
from datetime import datetime


# =============================================
# AUTH MODELS
# =============================================


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=6, max_length=100)

    class Config:
        json_schema_extra = {
            "example": {"email": "user@example.com", "password": "password123"}
        }


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=100)
    full_name: str = Field(min_length=2, max_length=100)
    barangay: str = Field(min_length=2, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    role: str = Field(default="resident", pattern="^(resident|driver|admin)$")

    @field_validator("password")
    @classmethod
    def validate_password(cls, v):
        if not any(c.isupper() for c in v):
            raise ValueError("Password must contain at least one uppercase letter")
        if not any(c.isdigit() for c in v):
            raise ValueError("Password must contain at least one digit")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "email": "newuser@example.com",
                "password": "SecurePass123",
                "full_name": "John Doe",
                "barangay": "Barangay 1",
                "phone": "+639123456789",
                "role": "resident",
            }
        }


class UpdateProfileRequest(BaseModel):
    full_name: Optional[str] = Field(None, min_length=2, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    barangay: Optional[str] = Field(None, min_length=2, max_length=100)

    class Config:
        json_schema_extra = {
            "example": {
                "full_name": "Jane Doe",
                "phone": "+639123456789",
                "barangay": "Barangay 2",
            }
        }


class CreateUserRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=100)
    full_name: str = Field(min_length=2, max_length=100)
    role: str = Field(pattern="^(resident|driver|admin)$")
    barangay: Optional[str] = Field(None, min_length=2, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)


# =============================================
# TRACKING MODELS
# =============================================


class UpdateTrackingRequest(BaseModel):
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    current_barangay: str = Field(min_length=2, max_length=100)
    status: str = Field(pattern="^(active|offline)$")
    speed: Optional[float] = Field(None, ge=0)

    class Config:
        json_schema_extra = {
            "example": {
                "latitude": 6.1256,
                "longitude": 125.1814,
                "current_barangay": "Barangay 1",
                "status": "active",
                "speed": 45.5,
            }
        }


class SetTruckOfflineRequest(BaseModel):
    driver_id: str = Field(min_length=1)
    reason: Optional[str] = Field(None, max_length=500)


# =============================================
# REPORT MODELS
# =============================================


class SubmitReportRequest(BaseModel):
    barangay: str = Field(min_length=2, max_length=100)
    description: str = Field(min_length=10, max_length=1000)
    category: str = Field(pattern="^(pothole|flooding|garbage|streetlight|other)$")
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    address: Optional[str] = Field(None, max_length=500)
    photo_url: Optional[str] = Field(None, max_length=500)

    class Config:
        json_schema_extra = {
            "example": {
                "barangay": "Barangay 1",
                "description": "Large pile of garbage blocking the street",
                "category": "garbage",
                "latitude": 6.1256,
                "longitude": 125.1814,
                "address": "Main Street, Barangay 1",
                "photo_url": "https://example.com/photo.jpg",
            }
        }


class UpdateReportRequest(BaseModel):
    description: Optional[str] = Field(None, min_length=10, max_length=1000)
    category: Optional[str] = Field(
        None, pattern="^(pothole|flooding|garbage|streetlight|other)$"
    )
    status: Optional[str] = Field(None, pattern="^(pending|in_progress|resolved)$")


class UpdateReportStatusRequest(BaseModel):
    status: str = Field(pattern="^(pending|in_progress|resolved)$")
    notes: Optional[str] = Field(None, max_length=500)


# =============================================
# SCHEDULE MODELS
# =============================================


class CreateScheduleRequest(BaseModel):
    barangay: str = Field(min_length=2, max_length=100)
    waste_type: str = Field(min_length=2, max_length=100)
    time_slot: str = Field(min_length=5, max_length=20)  # e.g., "08:00-10:00"
    day_of_week: int = Field(ge=0, le=6)  # 0=Monday, 6=Sunday
    collection_type: Optional[str] = Field(
        None, pattern="^(biodegradable|non_biodegradable|residual|all)$"
    )
    start_time: Optional[str] = Field(None, max_length=20)
    end_time: Optional[str] = Field(None, max_length=20)

    class Config:
        json_schema_extra = {
            "example": {
                "barangay": "Barangay 1",
                "waste_type": "Biodegradable",
                "time_slot": "08:00-10:00",
                "day_of_week": 0,
                "collection_type": "biodegradable",
                "start_time": "08:00",
                "end_time": "10:00",
            }
        }


class UpdateScheduleRequest(BaseModel):
    barangay: Optional[str] = Field(None, min_length=2, max_length=100)
    waste_type: Optional[str] = Field(None, min_length=2, max_length=100)
    time_slot: Optional[str] = Field(None, min_length=5, max_length=20)
    day_of_week: Optional[int] = Field(None, ge=0, le=6)
    is_active: Optional[bool] = None


class AssignScheduleRequest(BaseModel):
    driver_id: str = Field(min_length=1)


# =============================================
# COLLECTION MODELS
# =============================================


class StartRouteRequest(BaseModel):
    schedule_id: str = Field(min_length=1)


class CollectStopRequest(BaseModel):
    waste_amount_kg: float = Field(ge=0, le=10000)
    biodegradable_kg: Optional[float] = Field(None, ge=0)
    non_biodegradable_kg: Optional[float] = Field(None, ge=0)
    recyclable_kg: Optional[float] = Field(None, ge=0)
    notes: Optional[str] = Field(None, max_length=500)


class SkipStopRequest(BaseModel):
    skip_reason: str = Field(min_length=5, max_length=500)


# =============================================
# NOTIFICATION MODELS
# =============================================


class CreateNotificationRequest(BaseModel):
    user_id: str = Field(min_length=1)
    title: str = Field(min_length=2, max_length=200)
    body: str = Field(min_length=5, max_length=1000)
    notification_type: str = Field(
        pattern="^(truck_alert|schedule|announcement|reward|report_update)$"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "user-uuid",
                "title": "Truck Arriving",
                "body": "Garbage truck will arrive in 30 minutes",
                "notification_type": "truck_alert",
            }
        }


class MarkNotificationReadRequest(BaseModel):
    is_read: bool


class MarkAllNotificationsReadRequest(BaseModel):
    user_id: str = Field(min_length=1)


# =============================================
# REWARD MODELS
# =============================================


class CreateRewardRequest(BaseModel):
    name: str = Field(min_length=2, max_length=100)
    description: str = Field(min_length=10, max_length=500)
    point_cost: int = Field(ge=1, le=100000)
    image_url: Optional[str] = Field(None, max_length=500)
    stock: int = Field(ge=0, le=10000)

    class Config:
        json_schema_extra = {
            "example": {
                "name": "Eco Bag",
                "description": "Reusable eco-friendly shopping bag",
                "point_cost": 500,
                "image_url": "https://example.com/eco-bag.jpg",
                "stock": 100,
            }
        }


class RedeemRewardRequest(BaseModel):
    reward_id: str = Field(min_length=1)


class UpdateRedemptionRequest(BaseModel):
    status: str = Field(pattern="^(pending|approved|redeemed)$")


# =============================================
# SURVEY MODELS
# =============================================


class SubmitSurveyRequest(BaseModel):
    rating: int = Field(ge=1, le=5)
    comment: Optional[str] = Field(None, max_length=1000)
    barangay: str = Field(min_length=2, max_length=100)

    class Config:
        json_schema_extra = {
            "example": {
                "rating": 5,
                "comment": "Great service!",
                "barangay": "Barangay 1",
            }
        }


# =============================================
# ANNOUNCEMENT MODELS
# =============================================


class CreateAnnouncementRequest(BaseModel):
    title: str = Field(min_length=2, max_length=200)
    body: str = Field(min_length=10, max_length=2000)
    target: str = Field(pattern="^(all|barangay|drivers)$")
    target_barangay: Optional[str] = Field(None, min_length=2, max_length=100)

    class Config:
        json_schema_extra = {
            "example": {
                "title": "Schedule Change",
                "body": "Collection schedule changed for next week",
                "target": "all",
                "target_barangay": None,
            }
        }


class UpdateAnnouncementRequest(BaseModel):
    title: Optional[str] = Field(None, min_length=2, max_length=200)
    body: Optional[str] = Field(None, min_length=10, max_length=2000)
    target: Optional[str] = Field(None, pattern="^(all|barangay|drivers)$")
    target_barangay: Optional[str] = Field(None, min_length=2, max_length=100)


# =============================================
# DRIVER MODELS
# =============================================


class UpdateDriverStatusRequest(BaseModel):
    status: str = Field(pattern="^(active|offline|on_break)$")


class AssignTruckRequest(BaseModel):
    plate_number: str = Field(min_length=2, max_length=20)


# =============================================
# USER MANAGEMENT MODELS
# =============================================


class UpdateUserRoleRequest(BaseModel):
    role: str = Field(pattern="^(resident|driver|admin)$")


class ToggleUserStatusRequest(BaseModel):
    is_active: Optional[bool] = None
