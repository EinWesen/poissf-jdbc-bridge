package de.einwesen.jdbc;

import java.sql.SQLException;

public class UnknownException extends SQLException {

	private static final long serialVersionUID = 7999777284506264818L;

	public UnknownException() {
	}

	public UnknownException(String reason) {
		super(reason);
	}

}
