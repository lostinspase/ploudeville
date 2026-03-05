from enum import Enum


class TaskRank(str, Enum):
    REQUIRED = "required"
    OPTIONAL = "optional"


class PayoutType(str, Enum):
    ALLOWANCE = "allowance"
    SCREEN_TIME = "screen_time"
    POINTS = "points"


class RewardType(str, Enum):
    ALLOWANCE = "allowance"
    SCREEN_TIME = "screen_time"
    PRIVILEGE = "privilege"


class CompletionStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class ScheduleCadence(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
