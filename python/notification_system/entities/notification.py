from dataclasses import dataclass

@dataclass
class Notification:
    user_id: str
    message: str
    notification_type: str  # Ví dụ: "push", "email"

    def validate(self):
      if not self.user_id:
        raise ValueError("User ID is required")
      if not self.message:
        raise ValueError("Message is required")
      if not self.notification_type:
          raise ValueError("Notification type is required")
      # Thêm các validation khác nếu cần