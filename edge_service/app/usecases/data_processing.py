from app.entities.agent_data import AgentData
from app.entities.processed_agent_data import ProcessedAgentData


def process_agent_data(agent_data: AgentData) -> ProcessedAgentData:
    """
    Аналізує дані акселерометра та класифікує стан дороги.
    """
    # Базове значення осі Z у стані спокою ~ 9.81 (гравітація)
    # Якщо машину сильно труснуло, це яма.
    
    z_value = agent_data.accelerometer.z
    
    if z_value < 5.0 or z_value > 15.0:
        state = "pit"  # Яма або вибоїна
    else:
        state = "normal" # Нормальна дорога

    # Повертаємо проаналізовані дані
    return ProcessedAgentData(
        road_state=state,
        agent_data=agent_data
    )