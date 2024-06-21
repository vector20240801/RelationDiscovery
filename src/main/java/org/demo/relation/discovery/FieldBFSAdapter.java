package org.demo.relation.discovery;

import org.demo.relation.discovery.bfs.FieldBFSCommander;

import java.io.Serializable;

/**
 * @author vector
 * @date 2023-08-29 12:49
 */
public class FieldBFSAdapter implements Serializable {
    private static final long serialVersionUID = 8947260906481103472L;

    public static void main(String[] args) {
        FieldBFSCommander.reloadTableRelation(args[0], args[1]);
    }
}
