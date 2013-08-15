package examples;


import java.io.Serializable;

public class Code implements Serializable {
	private static final long serialVersionUID = -4566791244188825605L;
	private int id;
	private String name;
	private String link;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	@Override
	public String toString() {
		return "Code [id=" + id + ", name=" + name + ", link=" + link + "]";
	}
}
