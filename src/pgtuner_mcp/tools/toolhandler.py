"""Abstract base class for tool handlers following the ToolHandler pattern."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool, ToolAnnotations


class ToolHandler(ABC):
    """
    Abstract base class for MCP tool handlers.

    This pattern allows for modular organization of tools where each handler
    is responsible for defining its schema and executing its logic.

    Subclasses must implement:
        - name: The unique tool name
        - description: Human-readable description
        - get_tool_definition(): Returns the Tool schema
        - run_tool(): Executes the tool logic

    Optional attributes for MCP Tool Annotations:
        - title: Human-readable title for UI display
        - read_only_hint: If True, tool doesn't modify environment (default: True for most query tools)
        - destructive_hint: If True, tool may perform destructive updates
        - idempotent_hint: If True, calling multiple times with same args has same effect
        - open_world_hint: If True, tool interacts with external entities

    Example:
        class MyToolHandler(ToolHandler):
            name = "my_tool"
            description = "Does something useful"
            title = "My Useful Tool"
            read_only_hint = True

            def get_tool_definition(self) -> Tool:
                return Tool(
                    name=self.name,
                    description=self.description,
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "param": {"type": "string", "description": "A parameter"}
                        },
                        "required": ["param"]
                    },
                    annotations=self.get_annotations()
                )

            async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
                result = do_something(arguments["param"])
                return [TextContent(type="text", text=result)]
    """

    name: str = ""
    description: str = ""
    # MCP Tool Annotation hints
    title: str | None = None
    read_only_hint: bool | None = None
    destructive_hint: bool | None = None
    idempotent_hint: bool | None = None
    open_world_hint: bool | None = None

    def get_annotations(self) -> ToolAnnotations | None:
        """
        Build ToolAnnotations from the handler's hint attributes.

        Returns:
            ToolAnnotations object or None if no annotations are set
        """
        # Only include annotations if at least one hint is set
        if all(
            v is None for v in [
                self.title,
                self.read_only_hint,
                self.destructive_hint,
                self.idempotent_hint,
                self.open_world_hint
            ]
        ):
            return None

        return ToolAnnotations(
            title=self.title,
            readOnlyHint=self.read_only_hint,
            destructiveHint=self.destructive_hint,
            idempotentHint=self.idempotent_hint,
            openWorldHint=self.open_world_hint
        )

    @abstractmethod
    def get_tool_definition(self) -> Tool:
        """
        Return the MCP Tool definition including input schema.

        Returns:
            Tool: The tool definition with name, description, and inputSchema
        """
        pass

    @abstractmethod
    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        """
        Execute the tool logic with the provided arguments.

        Args:
            arguments: Dictionary of arguments matching the input schema

        Returns:
            Sequence[TextContent]: The tool output as text content

        Raises:
            ValueError: If required arguments are missing or invalid
            Exception: If tool execution fails
        """
        pass

    def validate_required_args(
        self,
        arguments: dict[str, Any],
        required: list[str]
    ) -> None:
        """
        Validate that required arguments are present.

        Args:
            arguments: The arguments dictionary to validate
            required: List of required argument names

        Raises:
            ValueError: If any required argument is missing
        """
        missing = [arg for arg in required if arg not in arguments or arguments[arg] is None]
        if missing:
            raise ValueError(f"Missing required arguments: {', '.join(missing)}")

    def format_error(self, error: Exception) -> Sequence[TextContent]:
        """
        Format an error as TextContent for tool response.

        Args:
            error: The exception to format

        Returns:
            Sequence[TextContent]: Error message as text content
        """
        return [TextContent(type="text", text=f"Error: {str(error)}")]

    def format_result(self, result: str) -> Sequence[TextContent]:
        """
        Format a string result as TextContent.

        Args:
            result: The result string to format

        Returns:
            Sequence[TextContent]: Result as text content
        """
        return [TextContent(type="text", text=result)]

    def format_json_result(self, data: Any) -> Sequence[TextContent]:
        """
        Format data as JSON TextContent.

        Args:
            data: The data to serialize as JSON

        Returns:
            Sequence[TextContent]: JSON-formatted text content
        """
        import json
        return [TextContent(type="text", text=json.dumps(data, indent=2, default=str))]
